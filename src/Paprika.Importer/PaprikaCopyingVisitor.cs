using System.Diagnostics.Metrics;
using System.Runtime.CompilerServices;
using System.Threading.Channels;
using Nethermind.Core.Crypto;
using Nethermind.Serialization.Rlp;
using Nethermind.Trie;
using Paprika.Chain;
using Paprika.Utils;
using Keccak = Paprika.Crypto.Keccak;

namespace Paprika.Importer;

public class PaprikaCopyingVisitor : ITreeVisitor<PathContext>, IDisposable
{
    struct Item
    {
        private readonly ValueHash256 _account;

        // account
        private readonly Nethermind.Core.Account? _accountValue;

        // storage
        private readonly ValueHash256 _storage;
        private readonly byte[]? _data;

        public Item(ValueHash256 account, Nethermind.Core.Account accountValue)
        {
            _account = account;
            _accountValue = accountValue;
        }

        public Item(ValueHash256 account, ValueHash256 storage, byte[] data)
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

        var options = new BoundedChannelOptions(2_000_000)
        {
            SingleReader = true,
            SingleWriter = false,
            AllowSynchronousContinuations = false
        };

        _channel = Channel.CreateBounded<Item>(options);

        _batchSize = batchSize;
        _skipStorage = skipStorage;
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
        const int finalizationDepth = 16;

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

    private static Keccak AsPaprika(Hash256 keccak)
    {
        Unsafe.SkipInit(out Keccak k);
        keccak.Bytes.CopyTo(k.BytesAsSpan);
        return k;
    }

    private static Keccak AsPaprika(ValueHash256 keccak)
    {
        Unsafe.SkipInit(out Keccak k);
        keccak.Bytes.CopyTo(k.BytesAsSpan);
        return k;
    }

    public void Dispose() => _meter.Dispose();
    public bool IsFullDbScan => true;
    public bool ShouldVisit(in PathContext nodeContext, Hash256 nextNode) => true;

    public void VisitTree(in PathContext nodeContext, Hash256 rootHash, TrieVisitContext trieVisitContext)
    {
    }

    public void VisitMissingNode(in PathContext nodeContext, Hash256 nodeHash, TrieVisitContext trieVisitContext)
    {
        throw new Exception("The node is missing!");
    }

    public void VisitBranch(in PathContext nodeContext, TrieNode node, TrieVisitContext trieVisitContext)
    {
    }

    public void VisitExtension(in PathContext nodeContext, TrieNode node, TrieVisitContext trieVisitContext)
    {
    }


    public void VisitLeaf(in PathContext nodeContext, TrieNode node, TrieVisitContext trieVisitContext, ReadOnlySpan<byte> value)
    {
        var context = nodeContext.Add(node.Key!);
        var path = context.AsNibblePath;

        if (path.Length == 64)
        {
            ValueHash256 account = default;
            path.RawSpan.CopyTo(account.BytesAsSpan);

            _accountsVisitedGauge.Add(1);

            Rlp.ValueDecoderContext decoderContext = new Rlp.ValueDecoderContext(value);
            Add(new Item(account, Rlp.Decode<Nethermind.Core.Account>(ref decoderContext)));
        }
        else
        {
            ValueHash256 account = default;
            ValueHash256 storage = default;

            path.RawSpan.Slice(0, 32).CopyTo(account.BytesAsSpan);
            path.RawSpan.Slice(32).CopyTo(storage.BytesAsSpan);

            Rlp.ValueDecoderContext rlp = new Rlp.ValueDecoderContext(value);
            Add(new(account, storage, rlp.DecodeByteArray()));
        }
    }

    public void VisitCode(in PathContext nodeContext, Hash256 codeHash, TrieVisitContext trieVisitContext)
    {
    }
}
