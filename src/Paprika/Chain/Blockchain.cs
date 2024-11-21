using System.Buffers;
using System.CodeDom.Compiler;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Diagnostics.Metrics;
using System.Runtime.CompilerServices;
using System.Threading.Channels;
using Paprika.Crypto;
using Paprika.Data;
using Paprika.Merkle;
using Paprika.Store;
using Paprika.Utils;
using BitFilter = Paprika.Data.BitMapFilter<Paprika.Data.BitMapFilter.OfN<Paprika.Data.BitMapFilter.OfNSize128>>;

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

    private volatile ReadOnlyWorldStateAccessor? _accessor;

    // Metrics
    private readonly Meter _meter;
    private readonly Counter<long> _bloomMissedReads;
    private readonly Histogram<int> _cacheUsageState;
    private readonly Histogram<int> _cacheUsagePreCommit;
    private readonly Histogram<int> _prefetchCount;

    private readonly IMultiHeadChain _chain;
    private readonly IPreCommitBehavior _preCommit;
    private readonly CacheBudget.Options _cacheBudgetStateAndStorage;
    private readonly CacheBudget.Options _cacheBudgetPreCommit;
    private readonly Action? _beforeMetricsDisposed;
    private bool _verify;

    public Blockchain(IDb db, IPreCommitBehavior preCommit, TimeSpan? minFlushDelay = null,
        CacheBudget.Options cacheBudgetStateAndStorage = default,
        CacheBudget.Options cacheBudgetPreCommit = default,
        int? finalizationQueueLimit = null, Action? beforeMetricsDisposed = null)
    {
        _chain = db.OpenMultiHeadChain();
        _preCommit = preCommit;
        _cacheBudgetStateAndStorage = cacheBudgetStateAndStorage;
        _cacheBudgetPreCommit = cacheBudgetPreCommit;
        _beforeMetricsDisposed = beforeMetricsDisposed;

        // metrics
        _meter = new Meter("Paprika.Chain.Blockchain");

        _bloomMissedReads = _meter.CreateCounter<long>("Bloom missed reads", "Reads",
            "Number of reads that passed bloom but missed in dictionary");
        _cacheUsageState = _meter.CreateHistogram<int>("State transient cache usage per commit", "%",
            "How much used was the transient cache");
        _cacheUsagePreCommit = _meter.CreateHistogram<int>("PreCommit transient cache usage per commit", "%",
            "How much used was the transient cache");
        _prefetchCount = _meter.CreateHistogram<int>("Prefetch count",
            "Key count", "Keys prefetched in the background by the prefetcher");

        // pool
        _pool = new(1024, true, _meter);
    }

    public void VerifyDbIntegrityOnCommit()
    {
        _verify = true;
    }

    public int PoolAllocatedMB => _pool.AllocatedMB ?? int.MaxValue;

    /// <summary>
    /// Announces the last block number that was flushed to disk.
    /// </summary>
    public event EventHandler<(uint blockNumber, Keccak blockHash)> Flushed;

    /// <summary>
    /// The flusher failed.
    /// </summary>
    public event EventHandler<Exception> FlusherFailure;

    public IWorldState StartNew(Keccak parentKeccak) => new BlockState(_chain.Begin(parentKeccak), this);

    // public IRawState StartRaw()
    // {
    //     return new RawState(this, _db);
    // }

    public IReadOnlyWorldState StartReadOnly(Keccak keccak)
    {
        if (_chain.TryLeaseReader(keccak, out var reader))
        {
            return new ReadOnlyState(reader);
        }

        throw new KeyNotFoundException($"Not found state with {keccak}");
    }

    public static IReadOnlyWorldState StartReadOnlyLatestFromDb(IDb db) =>
        throw new NotImplementedException("Not implemented");

    public Task Finalize(Keccak keccak) => _chain.Finalize(keccak);

    private BitFilter CreateBitFilter() => BitMapFilter.CreateOfN<BitMapFilter.OfNSize128>(_pool);

    /// <summary>
    /// Represents a block that is a result of ExecutionPayload.
    /// </summary>
    private class BlockState : RefCountingDisposable, IWorldState, ICommit, IProvideDescription, IStateStats
    {
        /// <summary>
        /// A simple set filter to assert whether the given key was set in a given block, used to speed up getting the keys.
        /// </summary>
        private readonly BitFilter _filter;

        private readonly Dictionary<Keccak, int>? _stats;

        /// <summary>
        /// Stores information about contracts that should have their previous incarnations destroyed.
        /// </summary>
        private HashSet<Keccak> _destroyed = new();

        private readonly IHead _head;
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

        private CacheBudget _cacheBudgetStorageAndStage;
        private CacheBudget _cacheBudgetPreCommit;

        private Keccak? _hash;

        private int _dbReads;

        public BlockState(IHead head, Blockchain blockchain)
        {
            _head = head;
            _blockchain = blockchain;

            _filter = _blockchain.CreateBitFilter();
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

        public Keccak ParentHash => _head.ParentHash;

        /// <summary>
        /// Commits the block to the blockchain.
        /// </summary>
        public Keccak Commit(uint blockNumber)
        {
            CommitImpl(blockNumber);

            ReportCacheUsage(_blockchain._cacheBudgetStateAndStorage, _cacheBudgetStorageAndStage,
                _blockchain._cacheUsageState);
            ReportCacheUsage(_blockchain._cacheBudgetPreCommit, _cacheBudgetPreCommit,
                _blockchain._cacheUsagePreCommit);

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

        private void CommitImpl(uint blockNumber)
        {
            if (_prefetcher != null)
            {
                _prefetcher.BlockFurtherPrefetching();
                _blockchain._prefetchCount.Record(_prefetcher.PrefetchCount);
            }

            EnsureHash();

            var hash = _hash!.Value;

            if (hash == ParentHash)
            {
                if (hash == Keccak.EmptyTreeHash)
                {
                    return;
                }

                ThrowSameState();
            }

            BlockNumber = blockNumber;

            // Destroy contracts to destroy first.
            if (_destroyed.Count > 0)
            {
                foreach (var account in _destroyed)
                {
                    _head.Destroy(NibblePath.FromKey(account));
                }
            }

            // Apply state, storage then preCommit.
            ApplyImpl(_head, _state, _blockchain);
            ApplyImpl(_head, _storage, _blockchain);
            ApplyImpl(_head, _preCommit, _blockchain);

            _head.Commit(blockNumber, hash);

            // Cleanup
            ResetAllButHash();

            [DoesNotReturn]
            [StackTraceHidden]
            static void ThrowSameState() => throw new Exception("The same state as the parent is not handled now");
        }

        /// <summary>
        /// Applies this state directly on the <see cref="IBatch"/>
        /// without creating an in-memory representation of the committed state.
        /// </summary>
        public void ApplyRaw(IBatch batch)
        {
            _prefetcher?.BlockFurtherPrefetching();

            EnsureHash();

            var hash = _hash!.Value;

            var earlyReturn = false;

            if (hash == ParentHash)
            {
                if (hash == Keccak.EmptyTreeHash)
                {
                    earlyReturn = true;
                }
            }

            if (earlyReturn)
            {
                return;
            }

            ApplyImpl(batch, _state, _blockchain);
            ApplyImpl(batch, _storage, _blockchain);
            ApplyImpl(batch, _preCommit, _blockchain);
        }

        public void Reset()
        {
            _hash = ParentHash;
            ResetAllButHash();
        }

        private void ResetAllButHash()
        {
            _filter.Clear();
            _destroyed.Clear();

            _cacheBudgetStorageAndStage = _blockchain._cacheBudgetStateAndStorage.Build();
            _cacheBudgetPreCommit = _blockchain._cacheBudgetPreCommit.Build();

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
                return _prefetcher = new PreCommitPrefetcher(_preCommit, this, _blockchain._pool);
            }

            return null;
        }

        private class PreCommitPrefetcher : IDisposable, IPreCommitPrefetcher, IPrefetcherContext, IThreadPoolWorkItem
        {
            private volatile bool _prefetchPossible = true;

            private readonly ConcurrentQueue<(Keccak, Keccak)> _items = new();
            private readonly BitFilter _prefetched;
            private readonly PooledSpanDictionary _cache;
            private readonly BlockState _parent;
            private readonly BufferPool _pool;

            private const int Working = 1;
            private const int NotWorking = 0;
            private volatile int _working = NotWorking;
            private readonly Page _workspace;

            private static readonly Keccak JustAccount = Keccak.Zero;

            public PreCommitPrefetcher(PooledSpanDictionary cache, BlockState parent, BufferPool pool)
            {
                _cache = cache;
                _parent = parent;
                _pool = pool;
                _prefetched = _parent._blockchain.CreateBitFilter();
                _workspace = pool.Rent(false);
            }

            public bool CanPrefetchFurther => _prefetchPossible;

            public void PrefetchAccount(in Keccak account)
            {
                if (CanPrefetchFurther == false)
                    return;

                var accountHash = account.GetHashCodeUlong();

                if (ShouldPrefetch(accountHash) == false)
                {
                    return;
                }

                _items.Enqueue((account, JustAccount));
                EnsureRunning();
            }

            private void EnsureRunning()
            {
                if (_working == NotWorking)
                {
                    if (Interlocked.CompareExchange(ref _working, Working, NotWorking) == NotWorking)
                    {
                        ThreadPool.UnsafeQueueUserWorkItem(this, false);
                    }
                }
            }

            public void SpinTillPrefetchDone()
            {
                SpinWait.SpinUntil(() => _working == NotWorking);
            }

            private bool ShouldPrefetch(ulong hash) => _prefetched.AddAtomic(hash);

            public void PrefetchStorage(in Keccak account, in Keccak storage)
            {
                if (CanPrefetchFurther == false)
                    return;

                // Try account first
                var accountHash = account.GetHashCodeUlong();
                var prefetchAccount = ShouldPrefetch(accountHash);

                if (prefetchAccount)
                {
                    _items.Enqueue((account, JustAccount));
                }

                var storageHash = storage.GetHashCodeUlong();
                var prefetchStorage = ShouldPrefetch(accountHash ^ storageHash);

                if (prefetchStorage)
                {
                    _items.Enqueue((account, storage));
                }

                if (prefetchStorage || prefetchAccount)
                {
                    EnsureRunning();
                }
            }

            void IThreadPoolWorkItem.Execute()
            {
                while (_items.TryDequeue(out (Keccak account, Keccak storage) item))
                {
                    lock (_cache)
                    {
                        if (_prefetchPossible == false)
                        {
                            // We leave _working set to Working so that next Prefetch operations
                            // never ensure that a task is running.
                            return;
                        }

                        if (item.storage.Equals(JustAccount))
                        {
                            PreCommit.Prefetch(item.account, this);
                        }
                        else
                        {
                            PreCommit.Prefetch(item.account, item.storage, this);
                        }
                    }
                }

                _working = NotWorking;
            }

            private IPreCommitBehavior PreCommit => _parent._blockchain._preCommit;

            public int PrefetchCount { get; private set; }

            public void BlockFurtherPrefetching()
            {
                lock (_cache)
                {
                    // Just set the prefetch possible to false and return.
                    // As every operation in IThreadPoolWorkItem.Execute takes this lock, it's safe.
                    // This has one additional benefit. There's no need to worry about whether a worker runs or not atm.
                    _prefetchPossible = false;
                }
            }

            [SkipLocalsInit]
            public ReadOnlySpanOwner<byte> Get(scoped in Key key, TransformPrefetchedData transform)
            {
                if (CanPrefetchFurther == false)
                {
                    // Nothing more to do
                    return default;
                }

                var hash = GetHash(key);
                var keyWritten = key.WriteTo(stackalloc byte[key.MaxByteLength]);

                if (_cache.TryGet(keyWritten, hash, out var cached))
                {
                    return new ReadOnlySpanOwner<byte>(cached, null);
                }

                if (CanPrefetchFurther == false)
                {
                    // Cannot, return
                    return default;
                }

                // We can prefetch so scan the ancestors. No using as we'll return it
                var ancestor = _parent.GetFromHead(key);

                var span = ancestor.Span;

                // Transform data before storing them in the cache. This is done so that Decompress for example is run on
                // this thread, no on the one that marks paths as dirty.
                var transformed = transform(span, _workspace.Span, out var entryType);

                // Store the transformed so that, if a buffer reuse occurs in the transform it can be done before the next one is called.
                _cache.Set(keyWritten, hash, transformed, (byte)entryType);
                _parent._filter.AddAtomic(hash);
                PrefetchCount++;

                // The data are in cache, but it's easier and faster to return the owner from ancestors.
                if (ancestor.IsEmpty)
                {
                    // An empty ancestor can be disposed fast, and return immediately.
                    ancestor.Dispose();
                    return default;
                }

                // No dispose, the owner must live.
                return ancestor.Owner;
            }

            public void Dispose()
            {
                _pool.Return(_workspace);
                _prefetched.Return(_pool);
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

            return ReadStorage(owner.Span, destination);
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

            return ReadAccount(owner.Span);
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
            _filter.Add(hash);

            var k = key.WriteTo(stackalloc byte[key.MaxByteLength]);
            dict.Set(k, hash, payload, (byte)type);
        }

        private void AddToFilter(ulong hash)
        {
            _filter.AddAtomic(hash);
        }

        private void SetImpl(in Key key, in ReadOnlySpan<byte> payload0, in ReadOnlySpan<byte> payload1,
            EntryType type,
            PooledSpanDictionary dict)
        {
            // clean precalculated hash
            _hash = null;

            var hash = GetHash(key);
            AddToFilter(hash);

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

            var owner = TryGetLocal(key, keyWritten, hash, out var succeeded);
            if (succeeded)
                return owner.WithDepth(0);

            return GetFromHead(key);
        }

        private ReadOnlySpanOwnerWithMetadata<byte> GetFromHead(scoped in Key key)
        {
            if (_head.TryGet(key, out var span))
            {
                // No leasing here, assume everything it local and within lifetime of the head.
                return new ReadOnlySpanOwner<byte>(span, null).FromDatabase();
            }

            return default;
        }

        /// <summary>
        /// Tries to get the key only from this block, acquiring no lease as it assumes that the lease is taken.
        /// </summary>
        private ReadOnlySpanOwner<byte> TryGetLocal(scoped in Key key, scoped ReadOnlySpan<byte> keyWritten,
            ulong bloom, out bool succeeded)
        {
            var mayHave = _filter.MayContain(bloom);

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
            _xorMissed.Dispose();
            _prefetcher?.Dispose();
            _filter.Return(Pool);
            _head.Dispose();
        }

        public override string ToString() =>
            base.ToString() + ", " +
            $"{nameof(BlockNumber)}: {BlockNumber}, " +
            $"State: {_state}, " +
            $"Storage: {_storage}, " +
            $"PreCommit: {_preCommit}";

        public int DbReads => Volatile.Read(ref _dbReads);
    }

    public bool HasState(in Keccak keccak) => _chain.HasState(keccak);

    /// <summary>
    /// Represents a block that is a result of ExecutionPayload.
    /// </summary>
    private class ReadOnlyState(IHeadReader reader) : RefCountingDisposable, IReadOnlyWorldState
    {
        public uint BlockNumber => reader.Metadata.BlockNumber;

        public Keccak Hash => reader.Metadata.StateHash;

        public Span<byte> GetStorage(in Keccak address, in Keccak storage, Span<byte> destination)
        {
            var key = Key.StorageCell(NibblePath.FromKey(address), storage);

            using var owner = Get(key);
            return ReadStorage(owner.Span, destination);
        }

        public Account GetAccount(in Keccak address)
        {
            var key = Key.Account(NibblePath.FromKey(address));

            using var owner = Get(key);
            return ReadAccount(owner.Span);
        }

        [SkipLocalsInit]
        public ReadOnlySpanOwnerWithMetadata<byte> Get(scoped in Key key)
        {
            if (reader.TryGet(key, out var span))
            {
                reader.AcquireLease();
                return new ReadOnlySpanOwner<byte>(span, reader).FromDatabase();
            }

            // The value is not there, but it was walked through. Return the default;
            return default;
        }

        protected override void CleanUp() => reader.Dispose();

        public override string ToString() =>
            base.ToString() + ", " +
            $"{nameof(BlockNumber)}: {BlockNumber}";
    }

    public static ulong GetHash(in Key key) => key.GetHashCodeULong();

    public async ValueTask DisposeAsync()
    {
        await _chain.DisposeAsync();

        _accessor?.Dispose();

        // once the flushing is done and blocks are disposed, dispose the pool
        _pool.Dispose();

        // dispose metrics, but flush them last time before unregistering
        _beforeMetricsDisposed?.Invoke();
        _meter.Dispose();
    }

    //     /// <summary>
    //     /// The raw state implementation that provides a 1 layer of read-through caching with the last block.
    //     /// </summary>
    //     private class RawState : IRawState
    //     {
    //         private ArrayBufferWriter<byte> _prefixesToDelete = new();
    //         private readonly Blockchain _blockchain;
    //         private readonly IDb _db;
    //         private BlockState _current;
    //
    //         private bool _finalized;
    //
    //         public RawState(Blockchain blockchain, IDb db)
    //         {
    //             _blockchain = blockchain;
    //             _db = db;
    //             _current = new BlockState(Keccak.Zero, _db.BeginReadOnlyBatch(), [], _blockchain);
    //         }
    //
    //         public void Dispose()
    //         {
    //             if (!_finalized)
    //             {
    //                 ThrowNotFinalized();
    //                 return;
    //             }
    //
    //             _current.Dispose();
    //
    //             [DoesNotReturn]
    //             [StackTraceHidden]
    //             static void ThrowNotFinalized()
    //             {
    //                 throw new Exception("Finalize not called. You need to call it before disposing the raw state. " +
    //                                     "Otherwise it won't be preserved properly");
    //             }
    //         }
    //
    //         public Account GetAccount(in Keccak address) => _current.GetAccount(address);
    //
    //         public Span<byte> GetStorage(in Keccak address, in Keccak storage, Span<byte> destination) =>
    //             _current.GetStorage(address, in storage, destination);
    //
    //         public Keccak Hash { get; private set; }
    //
    //         public void SetBoundary(in NibblePath account, in Keccak boundaryNodeKeccak)
    //         {
    // #if SNAP_SYNC_SUPPORT
    //             var path = SnapSync.CreateKey(account, stackalloc byte[NibblePath.FullKeccakByteLength]);
    //             var payload = SnapSync.WriteBoundaryValue(boundaryNodeKeccak, stackalloc byte[SnapSync.BoundaryValueSize]);
    //
    //             _current.SetAccountRaw(path.UnsafeAsKeccak, payload);
    // #endif
    //         }
    //
    //         public void SetBoundary(in Keccak account, in NibblePath storage, in Keccak boundaryNodeKeccak)
    //         {
    // #if SNAP_SYNC_SUPPORT
    //             var path = SnapSync.CreateKey(storage, stackalloc byte[NibblePath.FullKeccakByteLength]);
    //             var payload = SnapSync.WriteBoundaryValue(boundaryNodeKeccak, stackalloc byte[SnapSync.BoundaryValueSize]);
    //             _current.SetStorage(account, path.UnsafeAsKeccak, payload);
    // #endif
    //         }
    //
    //
    //         public void SetAccount(in Keccak address, in Account account) => _current.SetAccount(address, account);
    //
    //         public void SetStorage(in Keccak address, in Keccak storage, ReadOnlySpan<byte> value) =>
    //             _current.SetStorage(address, storage, value);
    //
    //         public void DestroyAccount(in Keccak address) => _current.DestroyAccount(address);
    //
    //         public void RegisterDeleteByPrefix(in Key prefix)
    //         {
    //             var span = _prefixesToDelete.GetSpan(prefix.MaxByteLength);
    //             var written = prefix.WriteTo(span);
    //             _prefixesToDelete.Advance(written.Length);
    //         }
    //
    //         public void Commit()
    //         {
    //             ThrowOnFinalized();
    //
    //             Hash = _current.Hash;
    //
    //             using var batch = _db.BeginNextBatch();
    //
    //             DeleteByPrefixes(batch);
    //
    //             _current.ApplyRaw(batch);
    //             _current.Dispose();
    //
    //             batch.Commit(CommitOptions.DangerNoWrite);
    //
    //             var read = _db.BeginReadOnlyBatch();
    //
    //             _current = new BlockState(Keccak.Zero, read, [], _blockchain);
    //         }
    //
    //         private void DeleteByPrefixes(IBatch batch)
    //         {
    //             var prefixes = _prefixesToDelete.WrittenSpan;
    //             while (prefixes.IsEmpty == false)
    //             {
    //                 prefixes = Key.ReadFrom(prefixes, out var prefixToDelete);
    //                 batch.DeleteByPrefix(prefixToDelete);
    //             }
    //             _prefixesToDelete.ResetWrittenCount();
    //         }
    //
    //         public void Finalize(uint blockNumber)
    //         {
    //             ThrowOnFinalized();
    //
    //             using var batch = _db.BeginNextBatch();
    //             batch.SetMetadata(blockNumber, Hash);
    //             batch.Commit(CommitOptions.DangerNoWrite);
    //
    //             _finalized = true;
    //         }
    //
    //         private void ThrowOnFinalized()
    //         {
    //             if (_finalized)
    //             {
    //                 ThrowAlreadyFinalized();
    //             }
    //
    //             [DoesNotReturn]
    //             [StackTraceHidden]
    //             static void ThrowAlreadyFinalized()
    //             {
    //                 throw new Exception("This ras state has already been finalized!");
    //             }
    //         }
    //
    //         public ReadOnlySpanOwnerWithMetadata<byte> Get(scoped in Key key) => ((IReadOnlyWorldState)_current).Get(key);
    //     }

    public IReadOnlyWorldStateAccessor BuildReadOnlyAccessor()
    {
        return _accessor = new ReadOnlyWorldStateAccessor(_chain);
    }

    private class ReadOnlyWorldStateAccessor(IMultiHeadChain chain) : IReadOnlyWorldStateAccessor
    {
        public bool HasState(in Keccak keccak) => chain.HasState(keccak);

        public Account GetAccount(in Keccak rootHash, in Keccak address)
        {
            if (!chain.TryLeaseReader(rootHash, out var state))
            {
                return default;
            }

            var key = Key.Account(NibblePath.FromKey(address));

            try
            {
                if (state.TryGet(key, out var span) == false)
                {
                    span = default;
                }

                return ReadAccount(span);
            }
            finally
            {
                // Release
                state.Dispose();
            }
        }

        public Span<byte> GetStorage(in Keccak rootHash, in Keccak address, in Keccak storage, Span<byte> destination)
        {
            if (!chain.TryLeaseReader(rootHash, out var reader))
            {
                return default;
            }

            try
            {
                var key = Key.StorageCell(NibblePath.FromKey(address), storage);

                if (reader.TryGet(key, out var span) == false)
                {
                    span = default;
                }

                return ReadStorage(span, destination);
            }
            finally
            {
                // Release
                reader.Dispose();
            }
        }

        public void Dispose()
        {
        }
    }

    private static void ApplyImpl(IDataSetter batch, PooledSpanDictionary dict, Blockchain blockchain)
    {
        var preCommit = blockchain._preCommit;

        var page = blockchain._pool.Rent(false);
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
            blockchain._pool.Return(page);
        }
    }

    private static Account ReadAccount(ReadOnlySpan<byte> span)
    {
        // Check for emptiness
        if (span.IsEmpty)
            return new Account(0, 0);

        Account.ReadFrom(span, out var result);
        return result;
    }

    private static Span<byte> ReadStorage(ReadOnlySpan<byte> data, Span<byte> destination)
    {
        // Check the span emptiness

        if (data.IsEmpty)
            return Span<byte>.Empty;

        data.CopyTo(destination);
        return destination.Slice(0, data.Length);
    }
}
