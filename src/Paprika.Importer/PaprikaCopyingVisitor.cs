using System.Diagnostics;
using System.Diagnostics.Metrics;
using System.Runtime.CompilerServices;
using System.Threading.Channels;
using Nethermind.Core.Crypto;
using Nethermind.Trie;
using Paprika.Chain;
using Paprika.Utils;
using Keccak = Paprika.Crypto.Keccak;

namespace Paprika.Importer;

public class PaprikaCopyingVisitor : ITreeLeafVisitor, IDisposable
{
    private readonly Blockchain _blockchain;
    private readonly int _batchSize;
    private readonly int _expectedAccountCount;
    private readonly Channel<(ValueKeccak, Nethermind.Core.Account)> _channel;

    private readonly Meter _meter;
    private readonly MetricsExtensions.IAtomicIntGauge _accountsGauge;

    private int _accounts;

    public PaprikaCopyingVisitor(Blockchain blockchain, int batchSize, int expectedAccountCount)
    {
        _meter = new Meter("Paprika.Importer");

        _accountsGauge = _meter.CreateAtomicObservableGauge("Accounts imported", "%");

        _blockchain = blockchain;

        var options = new UnboundedChannelOptions
        { SingleReader = true, SingleWriter = true, AllowSynchronousContinuations = false };
        _channel = Channel.CreateUnbounded<(ValueKeccak, Nethermind.Core.Account)>(options);

        _batchSize = batchSize;
        _expectedAccountCount = expectedAccountCount;
    }

    public void VisitLeafAccount(in ValueKeccak account, Nethermind.Core.Account value)
    {
        var incremented = Interlocked.Increment(ref _accounts);

        // update occasionally
        if (incremented % 100 == 0)
        {
            _accountsGauge.Set(incremented / (_expectedAccountCount / 100));
        }

        Debug.Assert(_channel.Writer.TryWrite((account, value)));
    }

    public void Finish() => _channel.Writer.Complete();

    public async Task Copy()
    {
        var parent = Keccak.Zero;
        uint number = 1;

        var reader = _channel.Reader;

        while (await reader.WaitToReadAsync())
        {
            var i = 0;
            // dummy, for import only
            var child = Keccak.Compute(parent.BytesAsSpan);
            using var block = _blockchain.StartNew(parent, child, number);

            while (i < _batchSize && reader.TryRead(out var item))
            {
                i++;

                var addr = AsPaprika(item.Item1);
                var v = item.Item2;

                var codeHash = AsPaprika(v.CodeHash);
                var storageRoot = AsPaprika(v.StorageRoot);
                block.SetAccount(addr, new Account(v.Balance, v.Nonce, codeHash, storageRoot));
            }

            // commit & finalize
            block.Commit();
            _blockchain.Finalize(child);

            // update
            number++;
            parent = child;
        }
    }

    private static Keccak AsPaprika(Nethermind.Core.Crypto.Keccak keccak)
    {
        Unsafe.SkipInit(out Keccak k);
        keccak.Bytes.CopyTo(k.BytesAsSpan);
        return k;
    }

    private static Keccak AsPaprika(Nethermind.Core.Crypto.ValueKeccak keccak)
    {
        Unsafe.SkipInit(out Keccak k);
        keccak.Bytes.CopyTo(k.BytesAsSpan);
        return k;
    }

    public void VisitLeafStorage(in ValueKeccak account, in ValueKeccak storage, ReadOnlySpan<byte> value)
    {
        throw new NotImplementedException();
    }

    public void Dispose() => _meter.Dispose();
}