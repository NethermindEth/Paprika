using System.Diagnostics.Metrics;
using System.Runtime.CompilerServices;
using System.Threading.Channels;
using Nethermind.Core.Crypto;
using Nethermind.State;
using Nethermind.Trie;
using Paprika.Chain;
using Paprika.Utils;
using Keccak = Paprika.Crypto.Keccak;

namespace Paprika.Importer;

public class PaprikaRootValidatingVisitor : ITreeLeafVisitor, IDisposable
{
    private readonly Blockchain _blockchain;
    private readonly int _batchSize;
    private readonly Meter _meter;
    private readonly MetricsExtensions.IAtomicIntGauge _accountsGauge;

    private readonly Channel<(ValueKeccak keccak, Nethermind.Core.Account)> _channel;

    // paprika state
    private Keccak _blockHash = Keccak.Zero;
    private uint _blockNumber = 1;
    private readonly Queue<Keccak> _finalized = new();

    // Nethermind
    private readonly StateTree? _state;

    private int _accounts;

    public PaprikaRootValidatingVisitor(Blockchain blockchain, int batchSize = 1, bool constructNethermindStateTree = true)
    {
        _meter = new Meter("Paprika.Importer");

        _accountsGauge = _meter.CreateAtomicObservableGauge("Accounts imported", "count");
        _blockchain = blockchain;
        _batchSize = batchSize;
        _state = constructNethermindStateTree ? new StateTree() : null;

        var options = new UnboundedChannelOptions
        {
            SingleReader = true,
            SingleWriter = false
        };
        _channel = Channel.CreateUnbounded<(ValueKeccak keccak, Nethermind.Core.Account)>(options);
    }

    public void VisitLeafAccount(in ValueKeccak account, Nethermind.Core.Account value)
    {
        var incremented = Interlocked.Increment(ref _accounts);

        // update occasionally
        if (incremented % 100 == 0)
        {
            _accountsGauge.Set(incremented);
        }

        if (_channel.Writer.TryWrite((account.ToKeccak(), value)) == false)
        {
            throw new Exception("Should write");
        }
    }

    public async Task<Keccak> Copy()
    {
        var batch = new List<(ValueKeccak keccak, Nethermind.Core.Account)>(_batchSize);
        var reader = _channel.Reader;

        while (await reader.WaitToReadAsync())
        {
            while (reader.TryRead(out var item))
            {
                batch.Add(item);
                if (batch.Count == _batchSize)
                {
                    Write(batch);
                    batch.Clear();
                }
            }
        }

        if (batch.Count > 0)
        {
            Write(batch);
            batch.Clear();
        }

        // finalize the rest
        while (_finalized.TryDequeue(out var f))
        {
            _blockchain.Finalize(f);
        }

        return _blockHash;
    }

    private void Write(IEnumerable<(ValueKeccak keccak, Nethermind.Core.Account)> batch)
    {
        using var paprikaState = _blockchain.StartNew(_blockHash);

        foreach (var item in batch)
        {
            var (account, value) = item;

            // Paprika
            var paprikaAccount = new Account(value.Balance, value.Nonce, AsPaprika(value.CodeHash),
                AsPaprika(value.StorageRoot));
            paprikaState.SetAccount(AsPaprika(account), paprikaAccount);

            // Nethermind
            _state?.Set(account, value);
        }

        // Paprika
        _blockHash = paprikaState.Commit(_blockNumber++);
        Finalize(_blockHash);

        if (_state != null)
        {
            _state.UpdateRootHash();

            if (_blockHash != AsPaprika(_state.RootHash))
            {
                throw new Exception($"Hashes different at {_blockNumber}");
            }
        }
    }

    private void Finalize(Keccak parent)
    {
        _finalized.Enqueue(parent);
        if (_finalized.Count > 32)
        {
            var finalized = _finalized.Dequeue();
            _blockchain.Finalize(finalized);
        }
    }

    public void VisitLeafStorage(in ValueKeccak account, in ValueKeccak storage, ReadOnlySpan<byte> value)
    {
    }

    private static Keccak AsPaprika(Nethermind.Core.Crypto.Keccak keccak)
    {
        Unsafe.SkipInit(out Keccak k);
        keccak.Bytes.CopyTo(k.BytesAsSpan);
        return k;
    }

    private static Keccak AsPaprika(ValueKeccak keccak)
    {
        Unsafe.SkipInit(out Keccak k);
        keccak.Bytes.CopyTo(k.BytesAsSpan);
        return k;
    }

    public void Finish()
    {
        _channel.Writer.Complete();
    }

    public void Dispose()
    {
        _meter.Dispose();
    }
}