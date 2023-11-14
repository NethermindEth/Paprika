using System.Diagnostics.Metrics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Threading.Channels;
using Nethermind.Core.Crypto;
using Nethermind.Serialization.Rlp;
using Nethermind.Trie;
using Paprika.Chain;
using Paprika.Utils;
using Keccak = Paprika.Crypto.Keccak;

namespace Paprika.Importer;

public class PaprikaCopyingVisitor : ITreeLeafVisitor, IDisposable
{
    struct Item
    {
        private readonly ValueKeccak _account;

        // account
        private readonly Nethermind.Core.Account? _accountValue;

        // storage
        private readonly ValueKeccak _storage;
        private readonly byte[]? _data;

        public Item(ValueKeccak account, Nethermind.Core.Account accountValue)
        {
            _account = account;
            _accountValue = accountValue;
        }

        public Item(ValueKeccak account, ValueKeccak storage, byte[] data)
        {
            _account = account;
            _storage = storage;
            _data = data;
        }

        public bool IsAccount => _accountValue != null;

        public bool Apply(IWorldState block, bool skipStorage)
        {
            var addr = AsPaprika(_account);

            if (_accountValue != null)
            {
                var v = _accountValue;
                var codeHash = AsPaprika(v.CodeHash);

                var storageRoot = skipStorage ? AsPaprika(v.StorageRoot) : Keccak.EmptyTreeHash;

                // import account with empty tree hash so that it can be dirtied properly
                block.SetAccount(addr, new Account(v.Balance, v.Nonce, codeHash, storageRoot));
                return true;
            }

            if (skipStorage == false)
            {
                block.SetStorage(addr, AsPaprika(_storage), _data);
                return true;
            }

            return false;
        }
    }

    private readonly Blockchain _blockchain;
    private readonly int _batchSize;
    private readonly bool _skipStorage;
    private readonly Channel<Item> _channel;

    private readonly Meter _meter;

    private readonly MetricsExtensions.IAtomicIntGauge _accountsVisitedGauge;
    private readonly MetricsExtensions.IAtomicIntGauge _accountsAddedGauge;

    public PaprikaCopyingVisitor(Blockchain blockchain, int batchSize, bool skipStorage)
    {
        _meter = new Meter("Paprika.Importer");

        _accountsVisitedGauge = _meter.CreateAtomicObservableGauge("Accounts visited", "count");
        _accountsAddedGauge = _meter.CreateAtomicObservableGauge("Accounts added", "count");

        _blockchain = blockchain;

        var options = new BoundedChannelOptions(batchSize * 1000)
        {
            SingleReader = true,
            SingleWriter = false,
            AllowSynchronousContinuations = false
        };

        _channel = Channel.CreateBounded<Item>(options);

        _batchSize = batchSize;
        _skipStorage = skipStorage;
    }

    public void VisitLeafAccount(in ValueKeccak account, Nethermind.Core.Account value)
    {
        _accountsVisitedGauge.Add(1);
        Add(new Item(account, value));
    }

    public void VisitLeafStorage(in ValueKeccak account, in ValueKeccak storage, ReadOnlySpan<byte> value)
    {
        var span = MemoryMarshal.CreateSpan(ref MemoryMarshal.GetReference(value), value.Length);
        Rlp.ValueDecoderContext rlp = new Rlp.ValueDecoderContext(span);
        Add(new(account, storage, rlp.DecodeByteArray()));
    }

    private void Add(Item item)
    {
        while (_channel.Writer.TryWrite(item) == false)
        {
            Thread.Sleep(TimeSpan.FromSeconds(1));
        }
    }

    public void Finish() => _channel.Writer.Complete();

    public async Task<Keccak> Copy()
    {
        var parent = Keccak.Zero;
        uint number = 1;

        var reader = _channel.Reader;

        var batch = new Queue<Item>();
        var finalization = new Queue<Keccak>();
        const int finalizationDepth = 32;

        while (await reader.WaitToReadAsync())
        {
            await BuildBatch(reader, batch);

            using var block = _blockchain.StartNew(parent);

            var added = 0;
            while (batch.TryDequeue(out var item))
            {
                if (item.Apply(block, _skipStorage))
                {
                    if (item.IsAccount)
                    {
                        added += 1;
                    }
                }
            }

            _accountsAddedGauge.Add(added);

            // commit & finalize
            var hash = block.Commit(number);

            finalization.Enqueue(hash);

            if (finalization.Count == finalizationDepth)
            {
                _blockchain.Finalize(finalization.Dequeue());
            }

            // update
            number++;
            parent = hash;
        }

        while (finalization.TryDequeue(out var keccak))
        {
            _blockchain.Finalize(keccak);
        }

        return parent;
    }

    private async Task BuildBatch(ChannelReader<Item> reader, Queue<Item> batch)
    {
        while (await reader.WaitToReadAsync())
        {
            while (reader.TryRead(out var item))
            {
                batch.Enqueue(item);
                
                if (batch.Count == _batchSize)
                {
                    return;
                }
            }
        }
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

    public void Dispose() => _meter.Dispose();
}