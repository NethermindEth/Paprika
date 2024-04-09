using System.CodeDom.Compiler;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Diagnostics.Metrics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
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
    private readonly BufferPool _pool;

    private readonly object _blockLock = new();
    private readonly Dictionary<uint, List<CommittedBlockState>> _blocksByNumber = new();
    private readonly Dictionary<Keccak, CommittedBlockState> _blocksByHash = new();

    private readonly Channel<CommittedBlockState> _finalizedChannel;

    // metrics
    private readonly Meter _meter;
    private readonly Histogram<int> _flusherBlockPerS;
    private readonly Histogram<int> _flusherBlockApplicationInMs;
    private readonly Histogram<int> _flusherFlushInMs;
    private readonly Counter<long> _bloomMissedReads;
    private readonly Histogram<int> _cacheUsageState;
    private readonly Histogram<int> _cacheUsagePreCommit;
    private readonly MetricsExtensions.IAtomicIntGauge _flusherQueueCount;

    private readonly IDb _db;
    private readonly IPreCommitBehavior _preCommit;
    private readonly CacheBudget.Options _cacheBudgetStateAndStorage;
    private readonly CacheBudget.Options _cacheBudgetPreCommit;
    private readonly TimeSpan _minFlushDelay;
    private readonly Action? _beforeMetricsDisposed;
    private readonly Task _flusher;

    private uint _lastFinalized;

    private static readonly TimeSpan DefaultFlushDelay = TimeSpan.FromSeconds(1);

    public Blockchain(IDb db, IPreCommitBehavior preCommit, TimeSpan? minFlushDelay = null,
        CacheBudget.Options cacheBudgetStateAndStorage = default,
        CacheBudget.Options cacheBudgetPreCommit = default,
        int? finalizationQueueLimit = null, Action? beforeMetricsDisposed = null)
    {
        _db = db;
        _preCommit = preCommit;
        _cacheBudgetStateAndStorage = cacheBudgetStateAndStorage;
        _cacheBudgetPreCommit = cacheBudgetPreCommit;
        _minFlushDelay = minFlushDelay ?? DefaultFlushDelay;
        _beforeMetricsDisposed = beforeMetricsDisposed;

        if (finalizationQueueLimit == null)
        {
            _finalizedChannel = Channel.CreateUnbounded<CommittedBlockState>(new UnboundedChannelOptions
            {
                SingleReader = true,
                SingleWriter = true,
            });
        }
        else
        {
            _finalizedChannel = Channel.CreateBounded<CommittedBlockState>(
                new BoundedChannelOptions(finalizationQueueLimit.Value)
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
        _bloomMissedReads = _meter.CreateCounter<long>("Bloom missed reads", "Reads",
            "Number of reads that passed bloom but missed in dictionary");
        _cacheUsageState = _meter.CreateHistogram<int>("State transient cache usage per commit", "%",
            "How much used was the transient cache");
        _cacheUsagePreCommit = _meter.CreateHistogram<int>("PreCommit transient cache usage per commit", "%",
            "How much used was the transient cache");

        // pool
        _pool = new(1024, true, _meter);

        using var batch = _db.BeginReadOnlyBatch();
        _lastFinalized = batch.Metadata.BlockNumber;
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
                var timer = Stopwatch.StartNew();

                (uint _blocksByNumber, Keccak blockHash) last = default;

                while (timer.Elapsed < _minFlushDelay && reader.TryRead(out var block))
                {
                    last = (block.BlockNumber, block.Hash);

                    using var batch = _db.BeginNextBatch();

                    // apply
                    var application = Stopwatch.StartNew();

                    flushed.Add(block.BlockNumber);

                    var flushedTo = block.BlockNumber;

                    batch.SetMetadata(block.BlockNumber, block.Hash);

                    block.Apply(batch);

                    // only for debugging if needed
                    //block.Assert(batch);

                    application.Stop();
                    _flusherBlockApplicationInMs.Record((int)application.ElapsedMilliseconds);

                    // If there's something in the queue, don't flush. Flush here only where there's nothing to read from the reader.
                    var level = reader.TryPeek(out _) ? CommitOptions.DangerNoFlush : CommitOptions.FlushDataOnly;

                    // commit but no flush here, it's too heavy, the flush will come later
                    await batch.Commit(level);

                    // inform blocks about flushing
                    lock (_blockLock)
                    {
                        if (!_blocksByNumber.TryGetValue(flushedTo, out var removedBlocks))
                        {
                            ThrowMissingBlocks(flushedTo);
                        }

                        var cloned = removedBlocks.ToArray();
                        foreach (var removedBlock in cloned)
                        {
                            // dispose one to allow leases to do the count
                            removedBlock.Dispose();
                        }
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

                Flushed?.Invoke(this, last);

                if (timer.ElapsedMilliseconds > 0)
                {
                    _flusherBlockPerS.Record((int)(count * 1000 / timer.ElapsedMilliseconds));
                }
            }
        }
        catch (Exception e)
        {
            FlusherFailure?.Invoke(this, e);
            Console.WriteLine(e);
            throw;
        }

        [DoesNotReturn]
        [StackTraceHidden]
        static void ThrowMissingBlocks(uint flushedTo)
        {
            throw new Exception($"Missing blocks at block number {flushedTo}");
        }
    }

    /// <summary>
    /// Announces the last block number that was flushed to disk.
    /// </summary>
    public event EventHandler<(uint blockNumber, Keccak blockHash)> Flushed;

    /// <summary>
    /// The flusher failed.
    /// </summary>
    public event EventHandler<Exception> FlusherFailure;

    private void Add(CommittedBlockState state)
    {
        // allocate before lock
        var list = new List<CommittedBlockState> { state };

        lock (_blockLock)
        {
            if (_blocksByHash.TryGetValue(state.Hash, out var committed))
            {
                if (committed.BlockNumber == state.BlockNumber)
                {
                    // There is an already existing state at the same block number.
                    // Just accept it and dispose the added. 
                    state.MakeDiscardable();

                    state.Dispose();
                    return;
                }
            }

            // blocks by number first
            ref var blocks =
                ref CollectionsMarshal.GetValueRefOrAddDefault(_blocksByNumber, state.BlockNumber, out var exists);

            if (exists == false)
            {
                blocks = list;
            }
            else
            {
                blocks!.Add(state);
            }

            // blocks by hash
            _blocksByHash.Add(state.Hash, state);
        }
    }

    private void Remove(CommittedBlockState blockState)
    {
        lock (_blockLock)
        {
            // blocks by number, use remove first as usually it should be the case
            if (!_blocksByNumber.Remove(blockState.BlockNumber, out var blocks))
            {
                ThrowBlocksNotFound(blockState);
                return;
            }

            blocks.Remove(blockState);
            if (blocks.Count > 0)
            {
                // re-add only if not empty
                _blocksByNumber.Add(blockState.BlockNumber, blocks);
            }

            // blocks by hash
            _blocksByHash.Remove(blockState.Hash);
        }

        [DoesNotReturn]
        [StackTraceHidden]
        static void ThrowBlocksNotFound(CommittedBlockState blockState)
        {
            throw new Exception($"Blocks @ {blockState.BlockNumber} should not be empty");
        }
    }

    public IWorldState StartNew(Keccak parentKeccak)
    {
        lock (_blockLock)
        {
            var (batch, ancestors) = BuildBlockDataDependencies(parentKeccak);
            return new BlockState(parentKeccak, batch, ancestors, this);
        }
    }

    public IRawState StartRaw()
    {
        return new RawState(this, _db);
    }

    public IReadOnlyWorldState StartReadOnly(Keccak keccak)
    {
        lock (_blockLock)
        {
            var (batch, ancestors) = BuildBlockDataDependencies(keccak);
            return new ReadOnlyState(keccak, batch, ancestors);
        }
    }

    public IReadOnlyWorldState StartReadOnlyLatestFromDb()
    {
        var batch = _db.BeginReadOnlyBatch($"Blockchain dependency LATEST");
        return new ReadOnlyState(batch.Metadata.StateHash, batch, Array.Empty<CommittedBlockState>());
    }

    private (IReadOnlyBatch batch, CommittedBlockState[] ancestors) BuildBlockDataDependencies(Keccak parentKeccak)
    {
        parentKeccak = Normalize(parentKeccak);

        if (parentKeccak == Keccak.Zero)
        {
            return (EmptyReadOnlyBatch.Instance, Array.Empty<CommittedBlockState>());
        }

        // the most recent finalized batch
        var batch = _db.BeginReadOnlyBatchOrLatest(parentKeccak, "Blockchain dependency");

        // batch matches the parent, return
        if (batch.Metadata.StateHash == parentKeccak)
            return (batch, Array.Empty<CommittedBlockState>());

        // no match, find chain
        var ancestors = new List<CommittedBlockState>();
        while (batch.Metadata.StateHash != parentKeccak)
        {
            if (_blocksByHash.TryGetValue(parentKeccak, out var ancestor) == false)
            {
                ThrowParentStateNotFound(parentKeccak);
            }

            ancestor.AcquireLease(); // lease it!
            ancestors.Add(ancestor);
            parentKeccak = Normalize(ancestor.ParentHash);
        }

        return (batch, ancestors.ToArray());

        static Keccak Normalize(in Keccak keccak)
        {
            // pages are zeroed before, return zero on empty tree
            return keccak == Keccak.EmptyTreeHash ? Keccak.Zero : keccak;
        }

        [DoesNotReturn]
        [StackTraceHidden]
        static void ThrowParentStateNotFound(in Keccak parentKeccak)
        {
            throw new Exception(
                $"Failed to build dependencies. Parent state with hash {parentKeccak} was not found");
        }
    }

    public void Finalize(Keccak keccak)
    {
        Stack<CommittedBlockState> finalized;
        uint count;

        // gather all the blocks to finalize
        lock (_blockLock)
        {
            if (_blocksByHash.TryGetValue(keccak, out var block) == false)
            {
                ThrowFinalizedBlockMissing();
            }

            Debug.Assert(block.BlockNumber > _lastFinalized,
                "Block that is finalized should have a higher number than the last finalized");

            // gather all the blocks between last finalized and this.

            count = block.BlockNumber - _lastFinalized;

            finalized = new((int)count);
            for (var blockNumber = block.BlockNumber; blockNumber > _lastFinalized; blockNumber--)
            {
                // no need to acquire lease here, the block is already leased for the blockchain before Add(block)
                finalized.Push(block);
                if (_blocksByHash.TryGetValue(block.ParentHash, out block) == false)
                {
                    break;
                }
            }

            _lastFinalized += count;
        }

        // report count before actual writing to do no
        _flusherQueueCount.Add((int)count);

        // push them!
        var writer = _finalizedChannel.Writer;

        while (finalized.TryPop(out var block))
        {
            if (writer.TryWrite(block) == false)
            {
                // hard spin wait on breaching the size
                SpinWait.SpinUntil(() => writer.TryWrite(block));
            }
        }

        [DoesNotReturn]
        [StackTraceHidden]
        static void ThrowFinalizedBlockMissing()
        {
            throw new Exception("Block that is marked as finalized is not present");
        }
    }

    /// <summary>
    /// Represents a block that is a result of ExecutionPayload.
    /// </summary>
    private class BlockState : RefCountingDisposable, IWorldState, ICommit, IProvideDescription, IStateStats
    {
        [ThreadStatic] private static HashSet<ulong>? s_bloomCache;

        /// <summary>
        /// A simple bloom filter to assert whether the given key was set in a given block, used to speed up getting the keys.
        /// </summary>
        private HashSet<ulong> _bloom;

        private readonly Dictionary<Keccak, int>? _stats;

        /// <summary>
        /// Stores information about contracts that should have their previous incarnations destroyed.
        /// </summary>
        private HashSet<Keccak>? _destroyed;

        private readonly ReadOnlyBatchCountingRefs _batch;
        private readonly CommittedBlockState[] _ancestors;

        private readonly Blockchain _blockchain;

        /// <summary>
        /// The maps mapping accounts information, written in this block.
        /// </summary>
        private PooledSpanDictionary _state = null!;

        /// <summary>
        /// The maps mapping storage information, written in this block.
        /// </summary>
        private PooledSpanDictionary _storage = null!;

        /// <summary>
        /// The values set the <see cref="IPreCommitBehavior"/> during the <see cref="ICommit.Visit"/> invocation.
        /// It's both storage & state as it's metadata for the pre-commit behavior.
        /// </summary>
        private PooledSpanDictionary _preCommit = null!;

        private PreCommitPrefetcher? _prefetcher;

        private readonly DelayedMetrics.DelayedCounter<long, DelayedMetrics.LongIncrement> _xorMissed;
        private readonly CacheBudget _cacheBudgetStorageAndStage;
        private readonly CacheBudget _cacheBudgetPreCommit;

        private Keccak? _hash;

        private int _dbReads;

        public BlockState(Keccak parentStateRoot, IReadOnlyBatch batch, CommittedBlockState[] ancestors,
            Blockchain blockchain)
        {
            _batch = new ReadOnlyBatchCountingRefs(batch);

            _ancestors = ancestors;

            _blockchain = blockchain;

            ParentHash = parentStateRoot;

            _bloom = Interlocked.Exchange(ref s_bloomCache, null) ?? new HashSet<ulong>();
            _destroyed = null;
            _stats = new Dictionary<Keccak, int>();

            _hash = ParentHash;

            _cacheBudgetStorageAndStage = blockchain._cacheBudgetStateAndStorage.Build();
            _cacheBudgetPreCommit = blockchain._cacheBudgetPreCommit.Build();

            _xorMissed = _blockchain._bloomMissedReads.Delay();

            CreateDictionaries();
        }

        private void CreateDictionaries()
        {
            CreateDict(ref _state, Pool);
            CreateDict(ref _storage, Pool);
            CreateDict(ref _preCommit, Pool);
            return;

            // as pre-commit can use parallelism, make the pooled dictionaries concurrent friendly:
            // 1. make the dictionary preserve once written values, which means that it can repeatedly read and set without worrying of ordering operations
            // 2. set dictionary so that it allows concurrent readers
            static void CreateDict(ref PooledSpanDictionary dict, BufferPool pool)
            {
                // ReSharper disable once ConditionIsAlwaysTrueOrFalseAccordingToNullableAPIContract
                // ReSharper disable once UseNullPropagation
                if (dict != null)
                {
                    // dispose previous
                    dict.Dispose();
                }

                dict = new PooledSpanDictionary(pool, true);
            }
        }

        public Keccak ParentHash { get; }

        /// <summary>
        /// Commits the block to the blockchain.
        /// </summary>
        public Keccak Commit(uint blockNumber)
        {
            var committed = CommitImpl(blockNumber, false);

            ReportCacheUsage(_blockchain._cacheBudgetStateAndStorage, _cacheBudgetStorageAndStage,
                _blockchain._cacheUsageState);
            ReportCacheUsage(_blockchain._cacheBudgetPreCommit, _cacheBudgetPreCommit,
                _blockchain._cacheUsagePreCommit);

            if (committed != null)
            {
                _blockchain.Add(committed);
            }

            return Hash;
        }

        /// <summary>
        /// Reports the given cache usage.
        /// </summary>
        private static void ReportCacheUsage(in CacheBudget.Options budget, CacheBudget actual, Histogram<int> reportTo)
        {
            var total = budget.EntriesPerBlock;
            if (total <= 0)
            {
                // disabled, nothing to report
                return;
            }

            var percentageLeft = (double)actual.BudgetLeft / total * 100;
            var percentageUsed = 100 - percentageLeft;

            reportTo.Record((int)percentageUsed);
        }

        private CommittedBlockState? CommitImpl(uint blockNumber, bool raw)
        {
            _prefetcher?.BlockFurtherPrefetching();

            EnsureHash();

            var hash = _hash!.Value;

            bool earlyReturn = false;

            if (hash == ParentHash)
            {
                if (hash == Keccak.EmptyTreeHash)
                {
                    earlyReturn = true;
                }
                else if (!raw)
                {
                    ThrowSameState();
                }
            }

            if (earlyReturn)
            {
                return null;
            }

            BlockNumber = blockNumber;

            var xor = new Xor8(_bloom);

            // clean no longer used fields
            var data = new PooledSpanDictionary(Pool, false);

            // use append for faster copies as state and storage won't overwrite each other
            _state.CopyTo(data, OmitUseOnce, true);
            _storage.CopyTo(data, OmitUseOnce, true);

            // TODO: apply InspectBeforeApply here to reduce memory usage?
            _preCommit.CopyTo(data, OmitUseOnce);

            // Creation acquires the lease
            return new CommittedBlockState(xor, _destroyed, _blockchain, data, hash,
                ParentHash,
                blockNumber, raw);

            [DoesNotReturn]
            [StackTraceHidden]
            static void ThrowSameState()
            {
                throw new Exception("The same state as the parent is not handled now");
            }
        }

        /// <summary>
        /// Filters out entries that are of type <see cref="EntryType.UseOnce"/> as they should be used once only.
        /// </summary>
        private static bool OmitUseOnce(byte metadata) => metadata != (int)EntryType.UseOnce;

        public CommittedBlockState CommitRaw() => CommitImpl(0, true)!;

        public void Reset()
        {
            _hash = ParentHash;
            _bloom.Clear();
            _destroyed = null;

            CreateDictionaries();
        }

        IStateStats IWorldState.Stats => this;

        public IPreCommitPrefetcher? OpenPrefetcher()
        {
            if (_prefetcher != null)
            {
                throw new Exception("Prefetching already started");
            }

            if (_blockchain._preCommit.CanPrefetch)
            {
                return _prefetcher = new PreCommitPrefetcher(_preCommit, this);
            }

            return null;
        }

        private class PreCommitPrefetcher(PooledSpanDictionary cache, BlockState parent)
            : IDisposable, IPreCommitPrefetcher, IPrefetcherContext
        {
            private bool _prefetchPossible = true;

            private readonly HashSet<int> _accounts = new();
            private readonly HashSet<ulong> _cached = new();

            public bool CanPrefetchFurther => Volatile.Read(ref _prefetchPossible);

            public void PrefetchAccount(in Keccak account)
            {
                if (CanPrefetchFurther == false)
                    return;

                lock (cache)
                {
                    if (_prefetchPossible == false)
                    {
                        return;
                    }

                    if (_accounts.Add(account.GetHashCode()) == false)
                    {
                        // already prefetched
                        return;
                    }

                    parent._blockchain._preCommit.Prefetch(account, this);
                }
            }

            public void PrefetchStorage(in Keccak account, in Keccak storage)
            {
                // For now, just prefetch account
                PrefetchAccount(account);
            }

            public void BlockFurtherPrefetching()
            {
                lock (cache)
                {
                    _prefetchPossible = false;
                }

                foreach (var key in _cached)
                {
                    parent._bloom.Add(key);
                }
            }

            [SkipLocalsInit]
            public ReadOnlySpanOwnerWithMetadata<byte> Get(scoped in Key key)
            {
                var hash = GetHash(key);
                var keyWritten = key.WriteTo(stackalloc byte[key.MaxByteLength]);

                if (_cached.Contains(hash) && cache.TryGet(keyWritten, hash, out var data))
                {
                    // No ownership needed, it's all local here
                    var owner = new ReadOnlySpanOwner<byte>(data, null);
                    return new ReadOnlySpanOwnerWithMetadata<byte>(owner, 0);
                }

                return parent.TryGetAncestors(key, keyWritten, hash);
            }

            [SkipLocalsInit]
            public void Set(in Key key, in ReadOnlySpan<byte> payload, EntryType type)
            {
                var hash = GetHash(key);

                _cached.Add(hash);

                var k = key.WriteTo(stackalloc byte[key.MaxByteLength]);
                cache.Set(k, hash, payload, (byte)type);
            }

            public void Dispose()
            {
            }
        }

        public uint BlockNumber { get; private set; }

        public Keccak Hash
        {
            get
            {
                EnsureHash();
                return _hash!.Value;
            }
        }

        private void EnsureHash()
        {
            if (_hash == null)
            {
                _hash = _blockchain._preCommit.BeforeCommit(this, _cacheBudgetPreCommit);
            }
        }

        private BufferPool Pool => _blockchain._pool;

        [SkipLocalsInit]
        public void DestroyAccount(in Keccak address)
        {
            _hash = null;

            var searched = NibblePath.FromKey(address);

            var account = Key.Account(address);

            // set account to empty first
            _state.Set(account.WriteTo(stackalloc byte[account.MaxByteLength]), GetHash(account),
                ReadOnlySpan<byte>.Empty, (byte)EntryType.Persistent);

            Destroy(searched, _storage);
            Destroy(searched, _preCommit);

            _stats![address] = 0;

            _destroyed ??= new HashSet<Keccak>();
            _destroyed.Add(address);

            _blockchain._preCommit.OnAccountDestroyed(address, this);

            return;

            static void Destroy(NibblePath searched, PooledSpanDictionary dict)
            {
                foreach (var kvp in dict)
                {
                    Key.ReadFrom(kvp.Key, out var key);
                    if (key.Path.Equals(searched))
                    {
                        kvp.Destroy();
                    }
                }
            }
        }

        public Span<byte> GetStorage(in Keccak address, in Keccak storage, Span<byte> destination)
        {
            var key = Key.StorageCell(NibblePath.FromKey(address), storage);

            using var owner = Get(key);

            TryCache(key, owner, _storage);

            // check the span emptiness
            var data = owner.Span;
            if (data.IsEmpty)
                return Span<byte>.Empty;

            data.CopyTo(destination);
            return destination.Slice(0, data.Length);
        }

        /// <summary>
        /// Decides to whether put the value in a transient cache or in a persistent cache to speed
        /// up queries in next executions. 
        /// </summary>
        /// <param name="key"></param>
        /// <param name="owner"></param>
        /// <param name="dict"></param>
        /// <exception cref="NotImplementedException"></exception>
        private void TryCache(in Key key, in ReadOnlySpanOwnerWithMetadata<byte> owner, PooledSpanDictionary dict)
        {
            if (_cacheBudgetStorageAndStage.ShouldCache(owner))
            {
                SetImpl(key, owner.Span, EntryType.Cached, dict);
            }
        }

        public Account GetAccount(in Keccak address)
        {
            var key = Key.Account(NibblePath.FromKey(address));

            using var owner = Get(key);

            TryCache(key, owner, _state);

            // check the span emptiness
            if (owner.Span.IsEmpty)
                return new Account(0, 0);

            Account.ReadFrom(owner.Span, out var result);
            return result;
        }

        [SkipLocalsInit]
        public void SetAccount(in Keccak address, in Account account, bool newAccountHint = false)
        {
            var payload = account.WriteTo(stackalloc byte[Account.MaxByteCount]);
            SetAccountRaw(address, payload, newAccountHint);
        }

        private void SetAccountRaw(in Keccak address, Span<byte> payload, bool newAccountHint)
        {
            var path = NibblePath.FromKey(address);
            var key = Key.Account(path);

            SetImpl(key, payload, EntryType.Persistent, _state);

            if (newAccountHint)
            {
                _blockchain._preCommit.OnNewAccountCreated(address, this);
            }

            _stats!.RegisterSetAccount(address);
        }

        public void SetStorage(in Keccak address, in Keccak storage, ReadOnlySpan<byte> value)
        {
            var path = NibblePath.FromKey(address);
            var key = Key.StorageCell(path, storage);

            SetImpl(key, value, EntryType.Persistent, _storage);

            _stats!.RegisterSetStorageAccount(address);
        }

        [SkipLocalsInit]
        private void SetImpl(in Key key, in ReadOnlySpan<byte> payload, EntryType type, PooledSpanDictionary dict)
        {
            // clean precalculated hash
            _hash = null;

            var hash = GetHash(key);
            _bloom.Add(hash);

            var k = key.WriteTo(stackalloc byte[key.MaxByteLength]);
            dict.Set(k, hash, payload, (byte)type);
        }

        private void SetImpl(in Key key, in ReadOnlySpan<byte> payload0, in ReadOnlySpan<byte> payload1,
            EntryType type,
            PooledSpanDictionary dict)
        {
            // clean precalculated hash
            _hash = null;

            var hash = GetHash(key);
            _bloom.Add(hash);

            var k = key.WriteTo(stackalloc byte[key.MaxByteLength]);

            dict.Set(k, hash, payload0, payload1, (byte)type);
        }

        ReadOnlySpanOwnerWithMetadata<byte> ICommit.Get(scoped in Key key) => Get(key);

        void ICommit.Set(in Key key, in ReadOnlySpan<byte> payload, EntryType type) =>
            SetImpl(key, payload, type, _preCommit);

        void ICommit.Set(in Key key, in ReadOnlySpan<byte> payload0, in ReadOnlySpan<byte> payload1, EntryType type) =>
            SetImpl(key, payload0, payload1, type, _preCommit);

        public string Describe(Key.Predicate? predicate = null)
        {
            var writer = new StringWriter();
            var indented = new IndentedTextWriter(writer);
            indented.Indent = 1;

            writer.WriteLine("State:");
            _state.Describe(indented, predicate);

            writer.WriteLine("Storage:");
            _storage.Describe(indented, predicate);

            writer.WriteLine("PreCommit:");
            _preCommit.Describe(indented, predicate);

            return writer.ToString();
        }

        void ICommit.Visit(CommitAction action, TrieType type)
        {
            var dict = type == TrieType.State ? _state : _storage;

            foreach (var kvp in dict)
            {
                if (kvp.Metadata == (byte)EntryType.Persistent)
                {
                    Key.ReadFrom(kvp.Key, out var key);
                    action(key, kvp.Value);
                }
            }
        }

        IChildCommit ICommit.GetChild() => new ChildCommit(Pool, this);

        public IReadOnlyDictionary<Keccak, int> Stats => _stats!;

        class ChildCommit(BufferPool pool, ICommit parent) : RefCountingDisposable, IChildCommit
        {
            private readonly PooledSpanDictionary _dict = new(pool, true);

            [SkipLocalsInit]
            public ReadOnlySpanOwnerWithMetadata<byte> Get(scoped in Key key)
            {
                var hash = GetHash(key);
                var keyWritten = key.WriteTo(stackalloc byte[key.MaxByteLength]);

                if (_dict.TryGet(keyWritten, hash, out var result))
                {
                    AcquireLease();
                    return new ReadOnlySpanOwnerWithMetadata<byte>(new ReadOnlySpanOwner<byte>(result, this), 0);
                }

                // Don't nest, as reaching to parent should be easy.
                return parent.Get(key);
            }

            [SkipLocalsInit]
            public void Set(in Key key, in ReadOnlySpan<byte> payload, EntryType type)
            {
                var hash = GetHash(key);
                var keyWritten = key.WriteTo(stackalloc byte[key.MaxByteLength]);

                _dict.Set(keyWritten, hash, payload, (byte)type);
            }

            [SkipLocalsInit]
            public void Set(in Key key, in ReadOnlySpan<byte> payload0, in ReadOnlySpan<byte> payload1, EntryType type)
            {
                var hash = GetHash(key);
                var keyWritten = key.WriteTo(stackalloc byte[key.MaxByteLength]);

                _dict.Set(keyWritten, hash, payload0, payload1, (byte)type);
            }

            public void Commit()
            {
                foreach (var kvp in _dict)
                {
                    Key.ReadFrom(kvp.Key, out var key);
                    var type = (EntryType)kvp.Metadata;

                    // flush down only volatiles
                    if (type != EntryType.UseOnce)
                    {
                        parent.Set(key, kvp.Value, type);
                    }
                }
            }

            public IChildCommit GetChild() => new ChildCommit(pool, this);

            public IReadOnlyDictionary<Keccak, int> Stats =>
                throw new NotImplementedException("Child commits provide no stats");

            protected override void CleanUp()
            {
                _dict.Dispose();
            }

            public override string ToString() => _dict.ToString();
        }

        [SkipLocalsInit]
        private ReadOnlySpanOwnerWithMetadata<byte> Get(scoped in Key key)
        {
            var hash = GetHash(key);
            var keyWritten = key.WriteTo(stackalloc byte[key.MaxByteLength]);

            return TryGet(key, keyWritten, hash);
        }

        /// <summary>
        /// A recursive search through the block and its parent until null is found at the end of the weekly referenced
        /// chain.
        /// </summary>
        private ReadOnlySpanOwnerWithMetadata<byte> TryGet(scoped in Key key, scoped ReadOnlySpan<byte> keyWritten,
            ulong bloom)
        {
            var owner = TryGetLocal(key, keyWritten, bloom, out var succeeded);
            if (succeeded)
                return owner.WithDepth(0);

            return TryGetAncestors(key, keyWritten, bloom);
        }

        private ReadOnlySpanOwnerWithMetadata<byte> TryGetAncestors(scoped in Key key,
            scoped ReadOnlySpan<byte> keyWritten, ulong bloom)
        {
            ushort depth = 1;

            var destroyedHash = CommittedBlockState.GetDestroyedHash(key);

            // walk all the blocks locally
            foreach (var ancestor in _ancestors)
            {
                var owner = ancestor.TryGetLocal(key, keyWritten, bloom, destroyedHash, out var succeeded);
                if (succeeded)
                    return owner.WithDepth(depth);

                depth++;
            }

            return TryGetDatabase(key);
        }

        [SkipLocalsInit]
        private ReadOnlySpanOwnerWithMetadata<byte> TryGetDatabase(scoped in Key key)
        {
            // report db read
            Interlocked.Increment(ref _dbReads);

            if (_batch.TryGet(key, out var span))
            {
                // return leased batch
                _batch.AcquireLease();
                return new ReadOnlySpanOwner<byte>(span, _batch).FromDatabase();
            }

            // Return default as the value does not exist
            return default;
        }

        /// <summary>
        /// Tries to get the key only from this block, acquiring no lease as it assumes that the lease is taken.
        /// </summary>
        private ReadOnlySpanOwner<byte> TryGetLocal(scoped in Key key, scoped ReadOnlySpan<byte> keyWritten,
            ulong bloom, out bool succeeded)
        {
            var mayHave = _bloom.Contains(bloom);

            // check if the change is in the block
            if (!mayHave)
            {
                // if destroyed, return false as no previous one will contain it
                if (IsAccountDestroyed(key))
                {
                    succeeded = true;
                    return default;
                }

                succeeded = false;
                return default;
            }

            // First always try pre-commit as it may overwrite data.
            // Don't do it for the storage though! StorageCell entries are not modified by pre-commit! It can only read them!
            if (key.Type != DataType.StorageCell && _preCommit.TryGet(keyWritten, bloom, out var span))
            {
                // return with owned lease
                succeeded = true;
                AcquireLease();
                return new ReadOnlySpanOwner<byte>(span, this);
            }

            return TryGetLocalDict(key, keyWritten, bloom, out succeeded);
        }

        private ReadOnlySpanOwner<byte> TryGetLocalDict(scoped in Key key, scoped ReadOnlySpan<byte> keyWritten,
            ulong bloom, out bool succeeded)
        {
            // select the map to search for
            var dict = key.Type switch
            {
                DataType.Account => _state,
                DataType.StorageCell => _storage,
                _ => null
            };

            if (dict is not null && dict.TryGet(keyWritten, bloom, out var span))
            {
                // return with owned lease
                succeeded = true;
                AcquireLease();
                return new ReadOnlySpanOwner<byte>(span, this);
            }

            _xorMissed.Add(1);

            // if destroyed, return false as no previous one will contain it
            succeeded = IsAccountDestroyed(key);
            return default;
        }

        private bool IsAccountDestroyed(scoped in Key key)
        {
            if (_destroyed == null)
                return false;

            if (key.Path.Length != NibblePath.KeccakNibbleCount)
                return false;

            // it's either Account, Storage, or Merkle that is a storage
            return _destroyed.Contains(key.Path.UnsafeAsKeccak);
        }

        protected override void CleanUp()
        {
            _state.Dispose();
            _storage.Dispose();
            _preCommit.Dispose();
            _batch.Dispose();
            _xorMissed.Dispose();
            _prefetcher?.Dispose();

            // release all the ancestors
            foreach (var ancestor in _ancestors)
            {
                ancestor.Dispose();
            }

            ReturnCacheToPool();
            return;

            void ReturnCacheToPool()
            {
                var bloom = _bloom;
                _bloom = null!;
                ref var cache = ref s_bloomCache;
                if (cache is null)
                {
                    // Return the cache to be reused
                    bloom.Clear();
                    cache = bloom;
                }
            }
        }

        public override string ToString() =>
            base.ToString() + ", " +
            $"{nameof(BlockNumber)}: {BlockNumber}, " +
            $"State: {_state}, " +
            $"Storage: {_storage}, " +
            $"PreCommit: {_preCommit}";

        public int DbReads => Volatile.Read(ref _dbReads);

        public IEnumerable<(uint blockNumber, Keccak hash)> Ancestors =>
            _ancestors.Select(ancestor => (ancestor.BlockNumber, ancestor.Hash));
    }

    public bool HasState(in Keccak keccak)
    {
        lock (_blockLock)
        {
            if (_blocksByHash.ContainsKey(keccak))
                return true;

            if (_db.HasState(keccak))
                return true;

            return false;
        }
    }

    /// <summary>
    /// Represents a block that is a result of ExecutionPayload.
    /// </summary>
    private class CommittedBlockState : RefCountingDisposable
    {
        /// <summary>
        /// A faster filter constructed on block commit.
        /// </summary>
        private readonly Xor8 _xor;

        /// <summary>
        /// Stores information about contracts that should have their previous incarnations destroyed.
        /// </summary>
        private readonly HashSet<Keccak>? _destroyed;

        /// <summary>
        /// Stores the xor filter of <see cref="_destroyed"/> if any.
        /// </summary>
        private readonly Xor8? _destroyedXor;

        private readonly Blockchain _blockchain;

        /// <summary>
        /// All the state, storage and commitment that was committed.
        /// </summary>
        private readonly PooledSpanDictionary _committed;

        private readonly bool _raw;
        private bool _discardable;
        private readonly DelayedMetrics.DelayedCounter<long, DelayedMetrics.LongIncrement> _xorMissed;

        public CommittedBlockState(Xor8 xor, HashSet<Keccak>? destroyed, Blockchain blockchain,
            PooledSpanDictionary committed, Keccak hash, Keccak parentHash,
            uint blockNumber, bool raw)
        {
            _xor = xor;
            _destroyed = destroyed;
            _destroyedXor = _destroyed != null
                ? new Xor8(new HashSet<ulong>(_destroyed.Select(k => GetDestroyedHash(k))))
                : null;

            _blockchain = blockchain;
            _committed = committed;
            _raw = raw;
            Hash = hash;
            ParentHash = parentHash;
            BlockNumber = blockNumber;

            _xorMissed = _blockchain._bloomMissedReads.Delay();
        }

        public Keccak ParentHash { get; }

        public uint BlockNumber { get; private set; }

        public Keccak Hash { get; }

        public const ulong NonDestroyable = 0;
        public const ulong Destroyable = 1;

        public static ulong GetDestroyedHash(in Key key)
        {
            var path = key.Path;

            // Check if the path length qualifies.
            // The check for destruction is performed only for Account, Storage or Merkle-of-Storage that all have full paths. 
            if (path.Length != NibblePath.KeccakNibbleCount)
                return NonDestroyable;

            // Return ulonged hash.
            return GetDestroyedHash(path.UnsafeAsKeccak);
        }

        private static ulong GetDestroyedHash(in Keccak keccak) => keccak.GetHashCodeUlong() | Destroyable;

        /// <summary>
        /// Tries to get the key only from this block, acquiring no lease as it assumes that the lease is taken.
        /// </summary>
        public ReadOnlySpanOwner<byte> TryGetLocal(scoped in Key key, scoped ReadOnlySpan<byte> keyWritten,
            ulong bloom, ulong destroyedHash, out bool succeeded)
        {
            var mayHave = _xor.MayContain(bloom);

            // check if the change is in the block
            if (!mayHave)
            {
                // if destroyed, return false as no previous one will contain it
                if (IsAccountDestroyed(key, destroyedHash))
                {
                    succeeded = true;
                    return default;
                }

                succeeded = false;
                return default;
            }

            // first always try pre-commit as it may overwrite data
            if (_committed.TryGet(keyWritten, bloom, out var span))
            {
                // return with owned lease
                succeeded = true;
                AcquireLease();
                return new ReadOnlySpanOwner<byte>(span, this);
            }

            _xorMissed.Add(1);

            // if destroyed, return false as no previous one will contain it
            if (IsAccountDestroyed(key, destroyedHash))
            {
                succeeded = true;
                return default;
            }

            succeeded = false;
            return default;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private bool IsAccountDestroyed(scoped in Key key, ulong destroyed)
        {
            if (_destroyedXor == null || destroyed == NonDestroyable)
                return false;

            return CheckDestroyed(in key, destroyed);

            [MethodImpl(MethodImplOptions.NoInlining)]
            bool CheckDestroyed(in Key key, ulong destroyed)
            {
                return _destroyedXor.MayContain(destroyed) && _destroyed!.Contains(key.Path.UnsafeAsKeccak);
            }
        }

        protected override void CleanUp()
        {
            _xorMissed.Dispose();
            _committed.Dispose();

            if (_raw == false && _discardable == false)
            {
                _blockchain.Remove(this);
            }
        }

        public void Apply(IBatch batch)
        {
            if (_destroyed is { Count: > 0 })
            {
                foreach (var account in _destroyed)
                {
                    batch.Destroy(NibblePath.FromKey(account));
                }
            }

            Apply(batch, _committed);
        }

        private void Apply(IBatch batch, PooledSpanDictionary dict)
        {
            var preCommit = _blockchain._preCommit;

            var page = _blockchain._pool.Rent(false);
            try
            {
                var span = page.Span;

                foreach (var kvp in dict)
                {
                    if (kvp.Metadata == (byte)EntryType.Persistent)
                    {
                        Key.ReadFrom(kvp.Key, out var key);
                        var data = preCommit == null ? kvp.Value : preCommit.InspectBeforeApply(key, kvp.Value, span);
                        batch.SetRaw(key, data);
                    }
                }
            }
            finally
            {
                _blockchain._pool.Return(page);
            }
        }

        public override string ToString() =>
            base.ToString() + ", " +
            $"{nameof(BlockNumber)}: {BlockNumber}, " +
            $"Committed data: {_committed}, ";

        public void MakeDiscardable()
        {
            _discardable = true;
        }
    }

    /// <summary>
    /// Represents a block that is a result of ExecutionPayload.
    /// </summary>
    private class ReadOnlyState : RefCountingDisposable, IReadOnlyWorldState
    {
        private readonly ReadOnlyBatchCountingRefs _batch;
        private readonly CommittedBlockState[] _ancestors;

        public ReadOnlyState(Keccak stateRoot, IReadOnlyBatch batch, CommittedBlockState[] ancestors)
        {
            _batch = new ReadOnlyBatchCountingRefs(batch);
            _ancestors = ancestors;
            Hash = stateRoot;
        }

        public uint BlockNumber { get; private set; }

        public Keccak Hash { get; }

        public Span<byte> GetStorage(in Keccak address, in Keccak storage, Span<byte> destination)
        {
            var key = Key.StorageCell(NibblePath.FromKey(address), storage);

            using var owner = Get(key);

            // check the span emptiness
            var data = owner.Span;
            if (data.IsEmpty)
                return Span<byte>.Empty;

            data.CopyTo(destination);
            return destination.Slice(0, data.Length);
        }

        public Account GetAccount(in Keccak address)
        {
            var key = Key.Account(NibblePath.FromKey(address));

            using var owner = Get(key);

            // check the span emptiness
            if (owner.Span.IsEmpty)
                return new Account(0, 0);

            Account.ReadFrom(owner.Span, out var result);
            return result;
        }

        [SkipLocalsInit]
        public ReadOnlySpanOwnerWithMetadata<byte> Get(scoped in Key key)
        {
            var hash = GetHash(key);
            var keyWritten = key.WriteTo(stackalloc byte[key.MaxByteLength]);

            var result = TryGet(key, keyWritten, hash, out var succeeded);

            Debug.Assert(succeeded);
            return result;
        }

        /// <summary>
        /// A recursive search through the block and its parent until null is found at the end of the weekly referenced
        /// chain.
        /// </summary>
        private ReadOnlySpanOwnerWithMetadata<byte> TryGet(scoped in Key key, scoped ReadOnlySpan<byte> keyWritten,
            ulong bloom,
            out bool succeeded)
        {
            ushort depth = 1;

            var destroyedHash = CommittedBlockState.GetDestroyedHash(key);

            // walk all the blocks locally
            foreach (var ancestor in _ancestors)
            {
                var owner = ancestor.TryGetLocal(key, keyWritten, bloom, destroyedHash, out succeeded);
                if (succeeded)
                    return owner.WithDepth(depth);

                depth++;
            }

            if (_batch.TryGet(key, out var span))
            {
                // return leased batch
                succeeded = true;
                _batch.AcquireLease();
                return new ReadOnlySpanOwner<byte>(span, _batch).FromDatabase();
            }

            // report as succeeded operation. The value is not there but it was walked through.
            succeeded = true;
            return default;
        }

        protected override void CleanUp()
        {
            _batch.Dispose();

            // release all the ancestors
            foreach (var ancestor in _ancestors)
            {
                ancestor.Dispose();
            }
        }

        public override string ToString() =>
            base.ToString() + ", " +
            $"{nameof(BlockNumber)}: {BlockNumber}";
    }

    public static ulong GetHash(in Key key) => key.GetHashCodeULong();

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

    /// <summary>
    /// The raw state implementation that provides a 1 layer of read-through caching with the last block.
    /// </summary>
    private class RawState : IRawState
    {
        private readonly Blockchain _blockchain;
        private readonly IDb _db;
        private BlockState _current;

        private bool _finalized;

        public RawState(Blockchain blockchain, IDb db)
        {
            _blockchain = blockchain;
            _db = db;
            _current = new BlockState(Keccak.Zero, _db.BeginReadOnlyBatch(), [], _blockchain);
        }

        public void Dispose()
        {
            if (!_finalized)
            {
                ThrowNotFinalized();
                return;
            }

            _current.Dispose();

            [DoesNotReturn]
            [StackTraceHidden]
            static void ThrowNotFinalized()
            {
                throw new Exception("Finalize not called. You need to call it before disposing the raw state. " +
                                    "Otherwise it won't be preserved properly");
            }
        }

        public Account GetAccount(in Keccak address) => _current.GetAccount(address);

        public Span<byte> GetStorage(in Keccak address, in Keccak storage, Span<byte> destination) =>
            _current.GetStorage(address, in storage, destination);

        public Keccak Hash { get; private set; }

        public void SetBoundary(in NibblePath account, in Keccak boundaryNodeKeccak)
        {
#if SNAP_SYNC_SUPPORT
            var path = SnapSync.CreateKey(account, stackalloc byte[NibblePath.FullKeccakByteLength]);
            var payload = SnapSync.WriteBoundaryValue(boundaryNodeKeccak, stackalloc byte[SnapSync.BoundaryValueSize]);

            _current.SetAccountRaw(path.UnsafeAsKeccak, payload);
#endif
        }

        public void SetBoundary(in Keccak account, in NibblePath storage, in Keccak boundaryNodeKeccak)
        {
#if SNAP_SYNC_SUPPORT
            var path = SnapSync.CreateKey(storage, stackalloc byte[NibblePath.FullKeccakByteLength]);
            var payload = SnapSync.WriteBoundaryValue(boundaryNodeKeccak, stackalloc byte[SnapSync.BoundaryValueSize]);
            _current.SetStorage(account, path.UnsafeAsKeccak, payload);
#endif
        }


        public void SetAccount(in Keccak address, in Account account) => _current.SetAccount(address, account);

        public void SetStorage(in Keccak address, in Keccak storage, ReadOnlySpan<byte> value) =>
            _current.SetStorage(address, storage, value);

        public void DestroyAccount(in Keccak address) => _current.DestroyAccount(address);

        public void Commit()
        {
            ThrowOnFinalized();

            // Committing Nth block:
            // 1. open read tx as this block did
            // 2. open write tx and apply Nth block onto it
            // 3. start new N+1th block
            // 4. use read tx as the read + add the current as parent

            var read = _db.BeginReadOnlyBatch();

            Hash = _current.Hash;

            using var batch = _db.BeginNextBatch();

            var committed = _current.CommitRaw();
            committed.Apply(batch);
            _current.Dispose();

            batch.Commit(CommitOptions.DangerNoWrite);

            // Lease is taken automatically by the ctor of the block state, not needed
            //_current.AcquireLease();
            var ancestors = new[] { committed };

            _current = new BlockState(Keccak.Zero, read, ancestors, _blockchain);
        }

        public void Finalize(uint blockNumber)
        {
            ThrowOnFinalized();

            using var batch = _db.BeginNextBatch();
            batch.SetMetadata(blockNumber, Hash);
            batch.Commit(CommitOptions.DangerNoWrite);

            _finalized = true;
        }

        private void ThrowOnFinalized()
        {
            if (_finalized)
            {
                ThrowAlreadyFinalized();
            }

            [DoesNotReturn]
            [StackTraceHidden]
            static void ThrowAlreadyFinalized()
            {
                throw new Exception("This ras state has already been finalized!");
            }
        }

        public ReadOnlySpanOwnerWithMetadata<byte> Get(scoped in Key key) => ((IReadOnlyWorldState)_current).Get(key);
    }
}