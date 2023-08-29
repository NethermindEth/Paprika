using System.Collections.Concurrent;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Diagnostics.Metrics;
using System.Threading.Channels;
using Paprika.Crypto;
using Paprika.Data;
using Paprika.Merkle;
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
public class Blockchain : IAsyncDisposable
{
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

    public Blockchain(PagedDb db, IPreCommitBehavior? preCommit = null, TimeSpan? minFlushDelay = null,
        int? finalizationQueueLimit = null, Action? beforeMetricsDisposed = null)
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

                    // commit but no flush here, it's too heavy, the flush will come later
                    await batch.Commit(CommitOptions.DangerNoFlush);

                    // immediately, after commit, start a new readonly batch, otherwise pages won't be reused aggressively
                    // publish the reader to the blocks following up the flushed one
                    var readOnlyBatch = new ReadOnlyBatchCountingRefs(_db.BeginReadOnlyBatch("Flusher - History"));
                    if (_blocksByNumber.TryGetValue(flushedTo + 1, out var nextBlocksToFlushedOne) == false)
                    {
                        throw new Exception(
                            "The blocks that is marked as finalized has no descendant. Is it possible?");
                    }

                    foreach (var nextBlock in nextBlocksToFlushedOne)
                    {
                        // lease first to bump up the counter, then pass
                        readOnlyBatch.Lease();
                        nextBlock.SetParentReader(readOnlyBatch);
                    }

                    if (_blocksByNumber.TryRemove(flushedTo, out var removedBlocks) == false)
                        throw new Exception($"Missing blocks at block number {flushedTo}");

                    foreach (var removedBlock in removedBlocks)
                    {
                        // remove by hash as well
                        _blocksByHash.TryRemove(removedBlock.Hash, out _);
                        removedBlock.Dispose();
                    }

                    _flusherQueueCount.Subtract(1);
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
        if (_blocksByHash.TryGetValue(parentKeccak, out var parent))
        {
            return new Block(parentKeccak, parent, blockKeccak, blockNumber, this);
        }

        using var batch =
            new ReadOnlyBatchCountingRefs(_db.BeginReadOnlyBatch($"{nameof(Blockchain)}.{nameof(StartNew)}"));
        batch.Lease(); // add one for this using

        var parentBlockNumber = blockNumber - 1;
        if (batch.Metadata.BlockNumber == parentBlockNumber)
        {
            // block does the leasing itself
            return new Block(parentKeccak, batch, blockKeccak, blockNumber, this);
        }

        throw new Exception("There is no parent and the db is not aligned with the parent number");
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
            BatchId = batch.BatchId;
        }

        protected override void CleanUp() => _batch.Dispose();

        public Metadata Metadata { get; }

        public uint BatchId { get; }

        public void Lease() => TryAcquireLease();


        public bool TryGet(scoped in Key key, out ReadOnlySpan<byte> result)
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

        public override string ToString() => $"Counter: {Counter}, BatchId:{_batch.BatchId}";
    }

    /// <summary>
    /// Represents a block that is a result of ExecutionPayload.
    /// </summary>
    private class Block : RefCountingDisposable, IWorldState, ICommit
    {
        public Keccak Hash { get; }
        public Keccak ParentHash { get; }
        public uint BlockNumber { get; }

        /// <summary>
        /// A simple bloom filter to assert whether the given key was set in a given block,
        /// used to speed up getting the keys.
        /// </summary>
        private readonly BloomFilter _bloom;

        private readonly Blockchain _blockchain;

        /// <summary>
        /// All the pages rented for this block from the <see cref="BufferPool"/>.
        /// </summary>
        private readonly List<Page> _pages = new();

        /// <summary>
        /// The maps mapping accounts information, written in this block.
        /// </summary>
        private readonly PooledSpanDictionary _state;

        /// <summary>
        /// The maps mapping storage information, written in this block.
        /// </summary>
        private readonly PooledSpanDictionary _storage;

        /// <summary>
        /// The values set the <see cref="IPreCommitBehavior"/> during the <see cref="ICommit.Visit"/> invocation.
        /// It's both storage & state as it's metadata for the pre-commit behavior.
        /// </summary>
        private readonly PooledSpanDictionary _preCommit;

        /// <summary>
        /// The previous can point to either another <see cref="Block"/> as the parent,
        /// or <see cref="IReadOnlyBatch"/> if the parent has been already applied to the state after finalization.
        /// </summary>
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

            _state = new PooledSpanDictionary(Pool);
            _storage = new PooledSpanDictionary(Pool);
            _preCommit = new PooledSpanDictionary(Pool);

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
            // run pre-commit
            _blockchain._preCommit?.BeforeCommit(this);

            // acquires one more lease for this block as it is stored in the blockchain
            AcquireLease();

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

        public byte[] GetStorage(in Keccak address, in Keccak storage)
        {
            var key = Key.StorageCell(NibblePath.FromKey(address), storage);

            using var owner = Get(key);

            // check the span emptiness
            if (owner.Span.IsEmpty)
                return Array.Empty<byte>();

            return owner.Span.ToArray();
        }

        public Account GetAccount(in Keccak address)
        {
            var key = Key.Account(NibblePath.FromKey(address));

            using var owner = Get(key);

            // check the span emptiness
            if (owner.Span.IsEmpty)
                return default;

            Account.ReadFrom(owner.Span, out var result);
            return result;
        }

        private static int GetBloom(in Key key) => key.GetHashCode();

        public void SetAccount(in Keccak address, in Account account)
        {
            var path = NibblePath.FromKey(address);
            var key = Key.Account(path);

            var payload = account.WriteTo(stackalloc byte[Account.MaxByteCount]);

            SetImpl(key, payload, _state);
        }

        public void SetStorage(in Keccak address, in Keccak storage, ReadOnlySpan<byte> value)
        {
            var path = NibblePath.FromKey(address);
            var key = Key.StorageCell(path, storage);

            SetImpl(key, value, _storage);
        }

        private void SetImpl(in Key key, in ReadOnlySpan<byte> payload, PooledSpanDictionary dict)
        {
            var bloom = GetBloom(key);
            _bloom.Set(bloom);

            dict.Set(key.WriteTo(stackalloc byte[key.MaxByteLength]), bloom, payload);
        }

        ReadOnlySpanOwner<byte> ICommit.Get(scoped in Key key) => Get(key);

        void ICommit.Set(in Key key, in ReadOnlySpan<byte> payload) => SetImpl(key, payload, _preCommit);

        void ICommit.Visit(CommitAction action, TrieType type)
        {
            var dict = type == TrieType.State ? _state : _storage;

            foreach (var kvp in dict)
            {
                Key.ReadFrom(kvp.Key, out var key);
                action(key, kvp.Value);
            }
        }

        private ReadOnlySpanOwner<byte> Get(scoped in Key key)
        {
            AcquireLease();

            var bloom = GetBloom(key);
            var keyWritten = key.WriteTo(stackalloc byte[key.MaxByteLength]);
            var context = new Context(key, keyWritten, bloom);

            var result = TryGet(context, out var succeeded);
            if (succeeded)
                return result;

            // slow path
            while (true)
            {
                result = TryGet(context, out succeeded);
                if (succeeded)
                    return result;

                Thread.Yield();
            }
        }

        readonly ref struct Context
        {
            public readonly Key Key;
            public readonly ReadOnlySpan<byte> KeyWritten;
            public readonly int Bloom;

            public Context(Key key, ReadOnlySpan<byte> keyWritten, int bloom)
            {
                Key = key;
                KeyWritten = keyWritten;
                Bloom = bloom;
            }
        }

        /// <summary>
        /// A recursive search through the block and its parent until null is found at the end of the weekly referenced
        /// chain.
        /// </summary>
        private ReadOnlySpanOwner<byte> TryGet(scoped in Context context, out bool succeeded)
        {
            // The lease of this is not needed.
            // The reason for that is that the caller did not .Dispose the reference held,
            // therefore the lease counter is up to date!
            var owner = TryGetLocalNoLease(context, out succeeded);
            if (succeeded)
                return owner;

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

            // the previous is now leased, all the methods are safe to be called
            if (previous is Block block)
            {
                return block.TryGet(context, out succeeded);
            }

            if (previous is IReadOnlyBatch batch)
            {
                if (batch.TryGet(context.Key, out var span))
                {
                    // return leased batch
                    succeeded = true;
                    return new ReadOnlySpanOwner<byte>(span, batch);
                }

                // report as succeeded operation. The value is not there but it was walked through.
                succeeded = true;
                return default;
            }

            throw new Exception($"The type of previous is not handled: {previous.GetType()}");
        }

        /// <summary>
        /// Tries to get the key only from this block, acquiring no lease as it assumes that the lease is taken.
        /// </summary>
        private ReadOnlySpanOwner<byte> TryGetLocalNoLease(scoped in Context context, out bool succeeded)
        {
            if (!_bloom.IsSet(context.Bloom))
            {
                succeeded = false;
                return default;
            }

            // select the map to search for 
            var dict = context.Key.Type switch
            {
                DataType.Account => _state,
                DataType.StorageCell => _storage,
                _ => _preCommit
            };

            if (dict.TryGet(context.KeyWritten, context.Bloom, out var span))
            {
                // return with owned lease
                succeeded = true;
                return new ReadOnlySpanOwner<byte>(span, this);
            }

            succeeded = false;
            return default;
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
            Apply(batch, _state);
            Apply(batch, _storage);
            Apply(batch, _preCommit);
        }

        private static void Apply(IBatch batch, PooledSpanDictionary dict)
        {
            foreach (var kvp in dict)
            {
                Key.ReadFrom(kvp.Key, out var key);
                batch.SetRaw(key, kvp.Value);
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

        Debugger.Log(0, null, $"Next free page: {_db.NextFreePage}\n\n");

        // dispose metrics, but flush them last time before unregistering
        _beforeMetricsDisposed?.Invoke();
        _meter.Dispose();
    }
}