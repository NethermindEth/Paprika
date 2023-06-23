using System.Collections.Concurrent;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Diagnostics.Metrics;
using System.Threading.Channels;
using Nethermind.Int256;
using Paprika.Crypto;
using Paprika.Data;
using Paprika.Store;
using Paprika.Utils;

namespace Paprika.Chain;

/// <summary>
/// The blockchain is the main component of Paprika, that can deal with latest, safe and finalized blocks.
///
/// For latest and safe, it uses a notion of block, that allows switching heads, querying from different heads etc.
/// For the finalized blocks, they are queued to a <see cref="Channel"/> that is consumed by a flushing mechanism
/// using the <see cref="PagedDb"/>.
/// </summary>
/// <remarks>
/// The current implementation assumes a single threaded access. For multi-threaded, some adjustments will be required.
/// The following should be covered:
/// 1. reading a state at a given time based on the root. Should never fail.
/// 2. TBD
/// </remarks>
public class Blockchain : IAsyncDisposable
{
    public static readonly Keccak GenesisHash = Keccak.Zero;

    // allocate 1024 pages (4MB) at once
    private readonly BufferPool _pool = new(1024);

    // It's unlikely that there will be many blocks per number as it would require the network to be heavily fragmented. 
    private readonly ConcurrentDictionary<uint, Block[]> _blocksByNumber = new();
    private readonly ConcurrentDictionary<Keccak, Block> _blocksByHash = new();
    private readonly Channel<Block> _finalizedChannel;

    // metrics
    private readonly Meter _meter;
    private readonly Histogram<int> _flusherBlockPerS;
    private readonly Histogram<int> _flusherBlockApplicationInMs;
    private readonly Histogram<int> _flusherFlushInMs;
    private readonly MetricsExtensions.IAtomicIntGauge _flusherQueueCount;

    private readonly PagedDb _db;
    private readonly IPreCommitBehavior? _preCommit;
    private readonly TimeSpan _minFlushDelay;
    private readonly Action? _beforeMetricsDisposed;
    private readonly Task _flusher;

    private uint _lastFinalized;

    private static readonly TimeSpan DefaultFlushDelay = TimeSpan.FromSeconds(1);

    public Blockchain(PagedDb db, IPreCommitBehavior? preCommit = null, TimeSpan? minFlushDelay = null, int? finalizationQueueLimit = null, Action? beforeMetricsDisposed = null)
    {
        _db = db;
        _preCommit = preCommit;
        _minFlushDelay = minFlushDelay ?? DefaultFlushDelay;
        _beforeMetricsDisposed = beforeMetricsDisposed;

        if (finalizationQueueLimit == null)
        {
            _finalizedChannel = Channel.CreateUnbounded<Block>(new UnboundedChannelOptions
            {
                SingleReader = true,
                SingleWriter = true,
            });
        }
        else
        {
            _finalizedChannel = Channel.CreateBounded<Block>(new BoundedChannelOptions(finalizationQueueLimit.Value)
            {
                SingleReader = true,
                SingleWriter = true,
                FullMode = BoundedChannelFullMode.Wait,
            });
        }

        var genesis = new Block(GenesisHash, new ReadOnlyBatchCountingRefs(db.BeginReadOnlyBatch()), GenesisHash, 0,
            this);

        _blocksByNumber[0] = new[] { genesis };
        _blocksByHash[GenesisHash] = genesis;

        _flusher = FlusherTask();

        // metrics
        _meter = new Meter("Paprika.Chain.Blockchain");

        _flusherBlockPerS = _meter.CreateHistogram<int>("Blocks stored / s", "Blocks/s",
            "The number of blocks stored by the flushing task in one second");
        _flusherBlockApplicationInMs = _meter.CreateHistogram<int>("Block data application in ms", "ms",
            "The amortized time it takes for one block to apply on PagedDb");
        _flusherFlushInMs = _meter.CreateHistogram<int>("FSYNC time", "ms",
            "The time it took to synchronize the file");
        _flusherQueueCount = _meter.CreateAtomicObservableGauge("Flusher queue size", "Blocks",
            "The number of the blocks in the flush queue");
    }

    /// <summary>
    /// The flusher method run as a reader of the <see cref="_finalizedChannel"/>.
    /// </summary>
    private async Task FlusherTask()
    {
        var reader = _finalizedChannel.Reader;

        try
        {
            while (await reader.WaitToReadAsync())
            {
                var flushed = new List<uint>();
                uint flushedTo = 0;
                var timer = Stopwatch.StartNew();

                while (timer.Elapsed < _minFlushDelay && reader.TryRead(out var block))
                {
                    using var batch = _db.BeginNextBatch();

                    // apply
                    var application = Stopwatch.StartNew();

                    flushed.Add(block.BlockNumber);
                    flushedTo = block.BlockNumber;

                    batch.SetMetadata(block.BlockNumber, block.Hash);

                    block.Apply(batch);

                    application.Stop();
                    _flusherBlockApplicationInMs.Record((int)application.ElapsedMilliseconds);

                    // commit but no flush here, it's too heavy
                    await batch.Commit(CommitOptions.DangerNoFlush);
                }

                timer.Stop();

                // measure
                var count = flushed.Count;

                if (count == 0)
                {
                    // nothing
                    continue;
                }

                var flushWatch = Stopwatch.StartNew();
                _db.Flush();
                _flusherFlushInMs.Record((int)flushWatch.ElapsedMilliseconds);

                if (timer.ElapsedMilliseconds > 0)
                {
                    _flusherBlockPerS.Record((int)(count * 1000 / timer.ElapsedMilliseconds));
                }

                _flusherQueueCount.Subtract(count);

                // publish the reader to the blocks following up the flushed one
                var readOnlyBatch = new ReadOnlyBatchCountingRefs(_db.BeginReadOnlyBatch());
                if (_blocksByNumber.TryGetValue(flushedTo + 1, out var nextBlocksToFlushedOne) == false)
                {
                    throw new Exception("The blocks that is marked as finalized has no descendant. Is it possible?");
                }

                foreach (var block in nextBlocksToFlushedOne)
                {
                    // lease first to bump up the counter, then pass
                    readOnlyBatch.Lease();
                    block.SetParentReader(readOnlyBatch);
                }

                // clean the earliest blocks
                foreach (var flushedBlockNumber in flushed)
                {
                    if (_blocksByNumber.TryRemove(flushedBlockNumber, out var removed) == false)
                        throw new Exception($"Missing blocks at block number {flushedBlockNumber}");

                    foreach (var block in removed)
                    {
                        // remove by hash as well
                        _blocksByHash.TryRemove(block.Hash, out _);
                        block.Dispose();
                    }
                }
            }
        }
        catch (Exception e)
        {
            Console.WriteLine(e);
            throw;
        }
    }

    public IWorldState StartNew(Keccak parentKeccak, Keccak blockKeccak, uint blockNumber)
    {
        if (!_blocksByHash.TryGetValue(parentKeccak, out var parent))
            throw new Exception("The parent block must exist");

        // not added to dictionaries until Commit
        return new Block(parentKeccak, parent, blockKeccak, blockNumber, this);
    }

    public void Finalize(Keccak keccak)
    {
        // find the block to finalize
        if (_blocksByHash.TryGetValue(keccak, out var block) == false)
        {
            throw new Exception("Block that is marked as finalized is not present");
        }

        Debug.Assert(block.BlockNumber > _lastFinalized,
            "Block that is finalized should have a higher number than the last finalized");

        // gather all the blocks between last finalized and this.
        var count = block.BlockNumber - _lastFinalized;
        Stack<Block> finalized = new((int)count);
        for (var blockNumber = block.BlockNumber; blockNumber > _lastFinalized; blockNumber--)
        {
            // to finalize
            finalized.Push(block);

            if (block.TryGetParent(out block) == false)
            {
                // no next block, break
                break;
            }
        }

        // report count before actual writing to do no
        _flusherQueueCount.Add((int)count);

        var writer = _finalizedChannel.Writer;

        while (finalized.TryPop(out block))
        {
            if (writer.TryWrite(block) == false)
            {
                // hard spin wait on breaching the size
                SpinWait.SpinUntil(() => writer.TryWrite(block));
            }
        }

        _lastFinalized += count;
    }

    private class ReadOnlyBatchCountingRefs : RefCountingDisposable, IReadOnlyBatch
    {
        private readonly IReadOnlyBatch _batch;
        private const int LiveOnlyAsLongAsBlocksThatReferThisBatch = 0;

        public ReadOnlyBatchCountingRefs(IReadOnlyBatch batch) : base(LiveOnlyAsLongAsBlocksThatReferThisBatch)
        {
            _batch = batch;
            Metadata = batch.Metadata;
        }

        protected override void CleanUp() => _batch.Dispose();

        public Metadata Metadata { get; }

        public void Lease() => TryAcquireLease();

        public bool TryGet(in Key key, out ReadOnlySpan<byte> result)
        {
            if (!TryAcquireLease())
            {
                throw new Exception("Should be able to lease it!");
            }

            try
            {
                return _batch.TryGet(key, out result);
            }
            finally
            {
                Dispose();
            }
        }

        public void Report(IReporter reporter) =>
            throw new NotImplementedException("One should not report over a block");
    }

    /// <summary>
    /// Represents a block that is a result of ExecutionPayload, storing it in a in-memory trie
    /// </summary>
    private class Block : RefCountingDisposable, IWorldState, ICommit
    {
        public Keccak Hash { get; }
        public Keccak ParentHash { get; }
        public uint BlockNumber { get; }

        private readonly BloomFilter _bloom;

        private readonly Blockchain _blockchain;

        private readonly List<Page> _pages = new();
        private readonly List<InBlockMap> _maps = new();

        // one of: Block as the parent, or IReadOnlyBatch if flushed
        private RefCountingDisposable _previous;

        public Block(Keccak parentHash, RefCountingDisposable parent, Keccak hash, uint blockNumber,
            Blockchain blockchain)
        {
            _previous = parent;
            _blockchain = blockchain;

            Hash = hash;
            BlockNumber = blockNumber;
            ParentHash = parentHash;

            // rent pages for the bloom
            _bloom = new BloomFilter(Rent());

            parent.AcquireLease();
        }

        private Page Rent()
        {
            var page = Pool.Rent(true);
            _pages.Add(page);
            return page;
        }

        /// <summary>
        /// Commits the block to the block chain.
        /// </summary>
        public void Commit()
        {
            // acquires one more lease for this block as it is stored in the blockchain
            AcquireLease();

            // run pre-commit
            _blockchain._preCommit?.BeforeCommit(this);

            // set to blocks in number and in blocks by hash
            _blockchain._blocksByNumber.AddOrUpdate(BlockNumber,
                static (_, block) => new[] { block },
                static (_, existing, block) =>
                {
                    var array = existing;
                    Array.Resize(ref array, array.Length + 1);
                    array[^1] = block;
                    return array;
                }, this);

            _blockchain._blocksByHash.TryAdd(Hash, this);
        }

        private BufferPool Pool => _blockchain._pool;

        public UInt256 GetStorage(in Keccak account, in Keccak address)
        {
            var bloom = BloomForStorageOperation(account, address);
            var key = Key.StorageCell(NibblePath.FromKey(account), address);

            using var owner = Get(bloom, key);

            // check the span emptiness
            if (owner.Span.IsEmpty)
                return default;

            Serializer.ReadStorageValue(owner.Span, out var value);
            return value;
        }

        public Account GetAccount(in Keccak account)
        {
            var bloom = BloomForAccountOperation(account);
            var key = Key.Account(NibblePath.FromKey(account));

            using var owner = Get(bloom, key);

            // check the span emptiness
            if (owner.Span.IsEmpty)
                return default;

            Serializer.ReadAccount(owner.Span, out var result);
            return result;
        }

        private static int BloomForStorageOperation(in Keccak key, in Keccak address) =>
            key.GetHashCode() ^ address.GetHashCode();

        private static int BloomForAccountOperation(in Keccak key) => key.GetHashCode();

        public void SetAccount(in Keccak key, in Account account)
        {
            _bloom.Set(BloomForAccountOperation(key));

            var path = NibblePath.FromKey(key);

            Span<byte> payload = stackalloc byte[Serializer.BalanceNonceMaxByteCount];
            payload = Serializer.WriteAccount(payload, account);

            Set(Key.Account(path), payload);
        }

        public void SetStorage(in Keccak key, in Keccak address, UInt256 value)
        {
            _bloom.Set(BloomForStorageOperation(key, address));

            var path = NibblePath.FromKey(key);

            Span<byte> payload = stackalloc byte[Serializer.StorageValueMaxByteCount];
            payload = Serializer.WriteStorageValue(payload, value);

            Set(Key.StorageCell(path, address), payload);
        }

        public bool TryGet(in Key key, out ReadOnlySpanOwner<byte> result) => throw new NotImplementedException("Not implemented yet");

        public void Set(in Key key, in ReadOnlySpan<byte> payload)
        {
            InBlockMap map;

            if (_maps.Count == 0)
            {
                map = new InBlockMap(Rent());
                _maps.Add(map);
            }
            else
            {
                map = _maps[^1];
            }

            if (map.TrySet(key, payload))
            {
                return;
            }

            // not enough space, allocate one more
            map = new InBlockMap(Rent());
            _maps.Add(map);

            map.TrySet(key, payload);
        }

        public IKeyEnumerator GetEnumerator() => throw new NotImplementedException("Not implemented yet");

        private ReadOnlySpanOwner<byte> Get(int bloom, in Key key)
        {
            if (TryAcquireLease() == false)
            {
                throw new ObjectDisposedException("This block has already been disposed");
            }

            var result = TryGet(bloom, key, out var succeeded);
            if (succeeded)
                return result;

            // slow path
            while (true)
            {
                result = TryGet(bloom, key, out succeeded);
                if (succeeded)
                    return result;

                Thread.Yield();
            }
        }

        /// <summary>
        /// A recursive search through the block and its parent until null is found at the end of the weekly referenced
        /// chain.
        /// </summary>
        [OptimizationOpportunity(OptimizationType.CPU,
            "If bloom filter was stored in-line in block, not rented, it could be used without leasing to check " +
            "whether the value is there. Less contention over lease for sure")]
        private ReadOnlySpanOwner<byte> TryGet(int bloom, in Key key, out bool succeeded)
        {
            // The lease of this is not needed.
            // The reason for that is that the caller did not .Dispose the reference held,
            // therefore the lease counter is up to date!

            if (_bloom.IsSet(bloom))
            {
                // go from last to youngest to find the recent value
                for (int i = _maps.Count - 1; i >= 0; i--)
                {
                    if (_maps[i].TryGet(key, out var span))
                    {
                        // return with owned lease
                        succeeded = true;
                        return new ReadOnlySpanOwner<byte>(span, this);
                    }
                }
            }

            // this asset should be no longer leased
            ReleaseLeaseOnce();

            // search the parent, either previous block or a readonly batch
            var previous = Volatile.Read(ref _previous);

            if (previous.TryAcquireLease() == false)
            {
                // the previous was not possible to get a lease, should return and retry
                succeeded = false;
                return default;
            }

            if (previous is Block block)
            {
                // return leased, how to ensure that the lease is right
                return block.TryGet(bloom, key, out succeeded);
            }

            if (previous is IReadOnlyBatch batch)
            {
                if (batch.TryGet(key, out var span))
                {
                    // return leased batch
                    succeeded = true;
                    return new ReadOnlySpanOwner<byte>(span, batch);
                }
            }

            throw new Exception($"The type of previous is not handled: {previous.GetType()}");
        }

        protected override void CleanUp()
        {
            // return all the pages
            foreach (var page in _pages)
            {
                Pool.Return(page);
            }

            // it's ok to go with null here
            var previous = Interlocked.Exchange(ref _previous!, null);
            previous.Dispose();
        }

        public void Apply(IBatch batch)
        {
            foreach (var map in _maps)
            {
                map.Apply(batch);
            }
        }

        // ReSharper disable once SuggestBaseTypeForParameter
        public void SetParentReader(ReadOnlyBatchCountingRefs readOnlyBatch)
        {
            AcquireLease();

            var previous = Interlocked.Exchange(ref _previous, readOnlyBatch);
            // dismiss the previous block
            ((Block)previous).Dispose();

            ReleaseLeaseOnce();
        }

        public bool TryGetParent([MaybeNullWhen(false)] out Block value)
        {
            var previous = Volatile.Read(ref _previous);
            if (previous is Block block)
            {
                value = block;
                return true;
            }

            value = null;
            return false;
        }
    }

    public async ValueTask DisposeAsync()
    {
        // mark writer as complete
        _finalizedChannel.Writer.Complete();

        // await the flushing task
        await _flusher;

        // dispose all memoized blocks to please the ref-counting
        foreach (var (_, block) in _blocksByHash)
        {
            block.Dispose();
        }

        _blocksByHash.Clear();
        _blocksByNumber.Clear();

        // once the flushing is done and blocks are disposed, dispose the pool
        _pool.Dispose();

        // dispose metrics, but flush them last time before unregistering
        _beforeMetricsDisposed?.Invoke();
        _meter.Dispose();
    }
}