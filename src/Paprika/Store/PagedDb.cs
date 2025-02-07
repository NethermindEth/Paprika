//#define TRACKING_REUSED_PAGES

using NonBlocking;
using System.Diagnostics;
using System.Diagnostics.Metrics;
using Paprika.Chain;
using Paprika.Crypto;
using Paprika.Data;
using Paprika.Store.PageManagers;
using Paprika.Utils;
using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;

namespace Paprika.Store;

/// <summary>
/// The base class for page db implementations.
/// </summary>
/// <remarks>
/// Assumes a continuous memory allocation as it provides addressing based on the pointers.
/// </remarks>
public sealed partial class PagedDb : IPageResolver, IDb, IDisposable
{
    /// <summary>
    /// The number of roots kept in the history.
    /// At least two are required to make sure that the writing transaction does not overwrite the current root.
    /// </summary>
    /// <remarks>
    /// REORGS
    /// It can be set arbitrary big and used for handling reorganizations.
    /// If history depth is set to the max reorg depth, moving to previous block is just a single write transaction moving the root back.
    /// </remarks>
    private const int MinHistoryDepth = 2;

    public const string MeterName = "Paprika.Store.PagedDb";
    public const string DbSize = "DB Size";

    private readonly IPageManager _manager;
    private readonly byte _historyDepth;
    private long _lastRoot;
    private readonly RootPage[] _roots;

    // batches
    private readonly object _batchLock = new();
    private readonly List<ReadOnlyBatch> _batchesReadOnly = new();
    private object? _batchCurrent;

    // metrics
    private readonly Meter _meter;
    private readonly Counter<long> _reads;
    private readonly Counter<long> _writes;
    private readonly Counter<long> _commits;
    private readonly Histogram<float> _commitDuration;
    private readonly Histogram<int> _commitPageCountTotal;
    private readonly Histogram<int> _commitPageCountReused;
    private readonly Histogram<int> _commitPageCountNewlyAllocated;
    private readonly Histogram<int> _commitPageAbandoned;
    private readonly Histogram<int> _commitAbandonedSameBatch;
    private readonly MetricsExtensions.IAtomicIntGauge _dbSize;
    private readonly MetricsExtensions.IAtomicIntGauge _lowestReadTxBatch;
    private readonly MetricsExtensions.IAtomicIntGauge _lastWriteTxBatch;
    private const string? BatchIdName = "BatchId";

    // pooled objects
    private readonly BufferPool _pool;

#if TRACKING_REUSED_PAGES
    // reuse tracking
    private readonly Dictionary<DbAddress, uint> _registeredForReuse = new();
    private readonly MetricsExtensions.IAtomicIntGauge _reusablePages;
#endif

    /// <summary>
    /// Initializes the paged db.
    /// </summary>
    /// <param name="manager">The page manager.</param>
    /// <param name="historyDepth">The depth history represent how many blocks should be able to be restored from the past. Effectively,
    ///     a reorg depth. At least 2 are required</param>
    private PagedDb(IPageManager manager, byte historyDepth)
    {
        if (historyDepth < MinHistoryDepth)
            throw new ArgumentException($"{nameof(historyDepth)} should be bigger than {MinHistoryDepth}");

        _manager = manager;
        _historyDepth = historyDepth;
        _roots = new RootPage[historyDepth];
        _batchCurrent = null;

        RootInit();

        // Metrics
        _meter = new Meter(MeterName);
        _dbSize = _meter.CreateAtomicObservableGauge(DbSize, "MB", "The size of the database in MB");

        _reads = _meter.CreateCounter<long>("Reads", "Reads", "The number of reads db handles");
        _writes = _meter.CreateCounter<long>("Writes", "Writes", "The number of writes db handles");
        _commits = _meter.CreateCounter<long>("Commits", "Commits", "The number of batch commits db handles");
        _commitDuration =
            _meter.CreateHistogram<float>("Commit duration", "ms", "The time it takes to perform a commit");
        _commitPageCountTotal = _meter.CreateHistogram<int>("Commit page count (total)", "pages",
            "The total number of pages flushed during the commit");
        _commitPageCountReused = _meter.CreateHistogram<int>("Commit page count (reused)", "pages",
            "The number of pages reused");
        _commitPageCountNewlyAllocated = _meter.CreateHistogram<int>("Commit page count (new)", "pages",
            "The number of pages newly allocated");
        _commitPageAbandoned = _meter.CreateHistogram<int>("Abandoned pages count", "pages",
            "The number of pages registered for future reuse (abandoned)");
        _commitAbandonedSameBatch = _meter.CreateHistogram<int>("Written and abandoned in the same batch", "pages",
            "The number of pages written and then registered for future reuse");
        _lowestReadTxBatch = _meter.CreateAtomicObservableGauge($"Lowest read {BatchIdName}", BatchIdName,
            "The lowest BatchId that is locked by a read tx");
        _lastWriteTxBatch = _meter.CreateAtomicObservableGauge($"Last written {BatchIdName}", BatchIdName,
            "The last BatchId that was written by a batch");


#if TRACKING_REUSED_PAGES
        // Reuse tracking
        _reusablePages = _meter.CreateAtomicObservableGauge("Pages registered for reuse", "count",
            "The number of pages registered to be reused");
#endif
        // Pool
        _pool = new BufferPool(16, BufferPool.PageTracking.AssertCount, _meter);
    }

    public static PagedDb NativeMemoryDb(long size, byte historyDepth = 2) =>
        new(new NativeMemoryPageManager(size, historyDepth), historyDepth);

    public static PagedDb MemoryMappedDb(long size, byte historyDepth, string directory, bool flushToDisk = true) =>
        new(
            new MemoryMappedPageManager(size, historyDepth, directory,
                flushToDisk ? PersistenceOptions.FlushFile : PersistenceOptions.MMapOnly), historyDepth);

    public void Prefetch(DbAddress addr) => _manager.Prefetch(addr);

    private void ReportReads(long number) => _reads.Add(number);

    private void ReportWrites(long number) => _writes.Add(number);

    private void ReportCommit(TimeSpan elapsed)
    {
        _commits.Add(1);
        _commitDuration.Record((float)elapsed.TotalMilliseconds);
    }

    private void ReportDbSize(int megabytes) => _dbSize.Set(megabytes);

    private void ReportPageStatsPerCommit(int totalPageCount, int reused, int newlyAllocated, int abandonedCount,
        int registeredToReuseAfterWritingThisBatch)
    {
        _commitPageCountTotal.Record(totalPageCount);
        _commitPageCountReused.Record(reused);
        _commitPageCountNewlyAllocated.Record(newlyAllocated);
        _commitPageAbandoned.Record(abandonedCount);
        _commitAbandonedSameBatch.Record(registeredToReuseAfterWritingThisBatch);
    }

    private void RootInit()
    {
        // create all root pages for the history depth
        for (uint i = 0; i < _historyDepth; i++)
        {
            _roots[i] = new RootPage(_manager.GetAt(DbAddress.Page(i)));
        }

        var start = _roots[0];
        if (start.Data.NextFreePage < _historyDepth)
        {
            // The start root must have the properly number set to first free page
            start.Data.NextFreePage = DbAddress.Page(_historyDepth);

            // The start root should be empty tree hash.
            start.Data.Metadata = new Metadata(0, Keccak.EmptyTreeHash);
        }

        _lastRoot = 0;
        for (var i = 0; i < _historyDepth; i++)
        {
            var batchId = _roots[i].Header.BatchId;
            if (batchId > _lastRoot)
            {
                _lastRoot = batchId;
            }
        }
    }

    public double Megabytes
    {
        get
        {
            lock (_batchLock)
            {
                return GetRootSizeInMb(Root);
            }
        }
    }

    private RootPage Root => _roots[_lastRoot % _historyDepth];

    public uint NextFreePage => Root.Data.NextFreePage.Raw;

    public void Dispose()
    {
        _pool.Dispose();
        _manager.Dispose();
        _meter.Dispose();
    }

    /// <summary>
    /// Begins a batch representing the next block.
    /// </summary>
    /// <returns></returns>
    public IBatch BeginNextBatch() => BuildFromRoot(Root);

    IReadOnlyBatch IDb.BeginReadOnlyBatch(string name) => BeginReadOnlyBatch(name);

    public IVisitableReadOnlyBatch BeginReadOnlyBatch(string name = "")
    {
        lock (_batchLock)
        {
            var root = Root;
            return BeginReadOnlyBatch(name, root);
        }
    }

    public int CountReadOnlyBatches()
    {
        lock (_batchLock)
        {
            return _batchesReadOnly.Count;
        }
    }

    private ReadOnlyBatch BeginReadOnlyBatch(string name, in RootPage root)
    {
        Debug.Assert(Monitor.IsEntered(_batchLock));

        var copy = new RootPage(_pool.Rent(false));

        root.CopyTo(copy);

        var batch = new ReadOnlyBatch(this, copy, name);

        // Update the lowest read tx batch
        var batchId = (int)batch.BatchId;
        _lowestReadTxBatch.Set(_batchesReadOnly.Count == 0 ? batchId : Math.Min(_lowestReadTxBatch.Read(), batchId));

        _batchesReadOnly.Add(batch);
        return batch;
    }

    public IReadOnlyBatch BeginReadOnlyBatch(in Keccak stateHash, string name = "")
    {
        lock (_batchLock)
        {
            if (TryFindRoot(stateHash, out var root))
            {
                return BeginReadOnlyBatch(name, root);
            }

            ThrowNoMatchingPage(stateHash);
            return null!;
        }

        [DoesNotReturn]
        [StackTraceHidden]
        static void ThrowNoMatchingPage(in Keccak stateHash)
        {
            throw new Exception($"There is no root page with the given stateHash '{stateHash}'!");
        }
    }

    public IReadOnlyBatch BeginReadOnlyBatchOrLatest(in Keccak stateHash, string name = "")
    {
        lock (_batchLock)
        {
            if (TryFindRoot(stateHash, out var root))
            {
                return BeginReadOnlyBatch(name, root);
            }

            return BeginReadOnlyBatch(name, Root);
        }
    }

    public IReadOnlyBatch[] SnapshotAll(bool withoutOldest = false)
    {
        var batches = new List<IReadOnlyBatch>();

        var limit = withoutOldest ? _historyDepth - 1 : _historyDepth;

        lock (_batchLock)
        {
            for (var back = 0; back < limit; back++)
            {
                if (_lastRoot - back < 0)
                {
                    break;
                }

                var at = (_lastRoot - back) % _historyDepth;

                batches.Add(BeginReadOnlyBatch(nameof(SnapshotAll), _roots[at]));
            }
        }

        return batches.ToArray();
    }

    public bool HasState(in Keccak stateHash)
    {
        lock (_batchLock)
        {
            return TryFindRoot(stateHash, out _);
        }
    }

    /// <summary>
    /// Tries to find the root with the given <paramref name="stateHash"/>
    /// </summary>
    private bool TryFindRoot(in Keccak stateHash, out RootPage root)
    {
        Debug.Assert(Monitor.IsEntered(_batchLock));

        for (var back = 0; back < _historyDepth; back++)
        {
            if (_lastRoot - back < 0)
            {
                break;
            }

            var at = (_lastRoot - back) % _historyDepth;

            if (_roots[at].Data.Metadata.StateHash == stateHash)
            {
                root = _roots[at];
                return true;
            }
        }

        root = default;
        return false;
    }

    public int HistoryDepth => _historyDepth;

    public void Accept(IPageVisitor visitor)
    {
        var i = 0U;

        foreach (var root in _roots)
        {
            using (visitor.On(root, DbAddress.Page(i++)))
            {
                root.Accept(visitor, this);
            }
        }
    }

    public void VisitRoot(IPageVisitor visitor)
    {
        var root = Root;

        using (visitor.On(root, GetAddress(Root.AsPage())))
        {
            root.Accept(visitor, this);
        }
    }

    /// <summary>
    /// Allows to walk through the pages. No locks, no safety, use for dev purposes.
    /// </summary>
    /// <returns></returns>
    public IEnumerable<Page> UnsafeEnumerateNonRoot()
    {
        for (uint i = _historyDepth; i < Root.Data.NextFreePage; i++)
        {
            yield return _manager.GetAt(DbAddress.Page(i));
        }
    }

    private static int GetRootSizeInMb(RootPage root) =>
        (int)((long)root.Data.NextFreePage.Raw * Page.PageSize / 1024 / 1024);

    private void DisposeReadOnlyBatch(ReadOnlyBatch batch)
    {
        lock (_batchLock)
        {
            _batchesReadOnly.Remove(batch);
            _pool.Return(batch.Root.AsPage());

            // update metrics
            if (_batchesReadOnly.Count == 0)
            {
                _lowestReadTxBatch.Set(0);
            }
            else
            {
                _lowestReadTxBatch.Set((int)_batchesReadOnly.Min(b => b.BatchId));
            }
        }
    }

    private IBatch BuildFromRoot(RootPage current)
    {
        lock (_batchLock)
        {
            if (_batchCurrent != null)
            {
                ThrowOnlyOneBatch();
            }

            // prepare root
            var root = CreateNextRoot(current, _pool);

            // metrics
            _lastWriteTxBatch.Set((int)root.Header.BatchId);

            // select min batch across the one respecting history and the min of all the read-only batches
            var minBatch = CalculateMinBatchId(root);

            var batch = new Batch(this, root, minBatch);
            _batchCurrent = batch;
            return batch;
        }
    }

    [DoesNotReturn]
    [StackTraceHidden]
    private static void ThrowOnlyOneBatch() =>
        throw new Exception("There is another batch active at the moment. Commit the other first");

    [DoesNotReturn]
    [StackTraceHidden]
    private static void ThrowNoBatch() => throw new Exception("There is no active batch at the moment.");

    private void RemoveBatch(object batchToRemove, bool notThrowOnMissing = false)
    {
        lock (_batchLock)
        {
            if (ReferenceEquals(_batchCurrent, null))
            {
                if (notThrowOnMissing)
                    return;

                ThrowNoBatch();
            }

            if (ReferenceEquals(_batchCurrent, batchToRemove))
            {
                _batchCurrent = null;
            }
            else
            {
                ThrowOnlyOneBatch();
            }
        }
    }

    private bool IsBatchActive(object batch)
    {
        lock (_batchLock)
        {
            return ReferenceEquals(_batchCurrent, batch);
        }
    }

    /// <summary>
    /// Calculates the minimal batch id prior to which abandoned pages can be reused.
    /// </summary>
    /// <param name="root">The root page that is the current one.</param>
    /// <param name="omitReadOnlyTransactions">Whether the scan should omit live read only transactions.
    /// Should not be set to true unless <see cref="IMultiHeadChain"/> uses it.</param>
    /// <returns>The batch id defining the boundary of the reuse.</returns>
    private uint CalculateMinBatchId(RootPage root, bool omitReadOnlyTransactions = false)
    {
        Debug.Assert(Monitor.IsEntered(_batchLock), "Should be called only under lock");

        var rootBatchId = root.Header.BatchId;

        var minBatch = rootBatchId < _historyDepth ? 0 : rootBatchId - _historyDepth;

        if (omitReadOnlyTransactions == false)
        {
            foreach (var batch in _batchesReadOnly)
            {
                minBatch = Math.Min(batch.BatchId, minBatch);
            }
        }

        return minBatch;
    }

    private static RootPage CreateNextRoot(RootPage current, BufferPool pool)
    {
        var root = new RootPage(pool.Rent(false));
        current.CopyTo(root);
        root.Header.BatchId++;
        return root;
    }

    private DbAddress GetAddress(in Page page) => _manager.GetAddress(page);

    public Page GetAt(DbAddress address) => _manager.GetAt(address);

    /// <summary>
    /// Sets the new root but does not bump the _lastRoot that should be done in a lock.
    /// </summary>
    private DbAddress SetNewRoot(RootPage root)
    {
        var pageAddress = (_lastRoot + 1) % _historyDepth;
        var destination = _roots[pageAddress];
        root.CopyTo(destination);

        return DbAddress.Page((uint)pageAddress);
    }

    private void CommitNewRoot() => _lastRoot += 1;

    private sealed class ReadOnlyBatch(PagedDb db, RootPage root, string name)
        : IVisitableReadOnlyBatch, IReadOnlyBatchContext
    {
        [ThreadStatic] private static ConcurrentDictionary<Keccak, uint>? s_cache;

        private ConcurrentDictionary<Keccak, uint> _idCache = Interlocked.Exchange(ref s_cache, null) ?? new(
            Environment.ProcessorCount,
            RootPage.IdCacheLimit);

        public RootPage Root => root;

        private long _reads;
        private volatile bool _disposed;

        public void Dispose()
        {
            db.ReportReads(Volatile.Read(ref _reads));
            _disposed = true;
            db.DisposeReadOnlyBatch(this);

            ReturnCacheToPool();

            void ReturnCacheToPool()
            {
                var idCache = _idCache;
                _idCache = null!;
                ref var cache = ref s_cache;
                if (cache is null)
                {
                    // Return the cache to be reused
                    idCache.Clear();
                    cache = idCache;
                }
            }
        }

        public Metadata Metadata => root.Data.Metadata;

        public bool TryGet(scoped in Key key, out ReadOnlySpan<byte> result)
        {
            ObjectDisposedException.ThrowIf(_disposed, this);

            // Need to use interlocked as read batches can be used concurrently
            Interlocked.Increment(ref _reads);

            return root.TryGet(key, this, out result);
        }

        public void VerifyNoPagesMissing() => MissingPagesVisitor.VerifyNoPagesMissing(Root, db, this);

        public void Accept(IPageVisitor visitor) => Root.Accept(visitor, this);

        public uint BatchId => root.Header.BatchId;

        public IDictionary<Keccak, uint> IdCache
        {
            get
            {
                ObjectDisposedException.ThrowIf(_disposed, this);
                return _idCache;
            }
        }

        public void Prefetch(DbAddress addr) => db.Prefetch(addr);

        public Page GetAt(DbAddress address) => db._manager.GetAt(address);

        public override string ToString() => $"{nameof(ReadOnlyBatch)}, Name: {name}, BatchId: {BatchId}";
    }

    private sealed class Batch(PagedDb db, RootPage root, uint reusePagesOlderThanBatchId)
        : BatchBase(db), IBatch
    {
        private bool _verify;

        RootPage IReadOnlyBatch.Root { get; } = root;

        protected override RootPage Root { get; } = root;
        protected override uint ReusePagesOlderThanBatchId { get; } = reusePagesOlderThanBatchId;

        public override DbAddress GetAddress(Page page) => Db.GetAddress(page);

        [DebuggerStepThrough]
        public override Page GetAt(DbAddress address)
        {
            // Getting a page beyond root!
            var nextFree = Root.Data.NextFreePage;
            Debug.Assert(address < nextFree, $"Breached the next free page, NextFree: {nextFree}, retrieved {address}");

            return Db.GetAt(address);
        }

        public void VerifyDbPagesOnCommit()
        {
            _verify = true;
        }

        public void VerifyNoPagesMissing() => MissingPagesVisitor.VerifyNoPagesMissing(Root, Db, this);

        public async ValueTask Commit(CommitOptions options)
        {
            if (db.IsBatchActive(this) == false)
            {
                throw new Exception("This batch is not active");
            }

            var watch = Stopwatch.StartNew();

            CheckDisposed();

            // memoize the abandoned so that it's preserved for future uses
            var abandoned = MemoizeAbandoned();

            if (_verify)
            {
                VerifyNoPagesMissing();
            }

            // report metrics
            Db.ReportPageStatsPerCommit(Written.Count, Metrics.PagesReused, Metrics.PagesAllocated, abandoned,
                Metrics.RegisteredToReuseAfterWritingThisBatch);

            Db.ReportReads(Metrics.Reads);
            Db.ReportWrites(Metrics.Writes);

#if TRACKING_REUSED_PAGES
            _db._reusablePages.Set(_db._registeredForReuse.Count);
#endif

            await Db._manager.WritePages(Written, options);

            var newRootPage = Db.SetNewRoot(Root);

            // report
            Db.ReportDbSize(GetRootSizeInMb(Root));

            await Db._manager.WriteRootPage(newRootPage, options);

            lock (Db._batchLock)
            {
                Db.CommitNewRoot();
                Db.RemoveBatch(this, true);
            }

            Db.ReportCommit(watch.Elapsed);
        }

        public override uint BatchId { get; } = root.Header.BatchId;

        protected override void DisposeImpl()
        {
            Db._pool.Return(Root.AsPage());
            Db.RemoveBatch(this, true);
        }
    }

    /// <summary>
    /// A base class for any <see cref="IBatch"/>-like object.
    /// </summary>
    private abstract class BatchBase : BatchContextBase, IDataSetter
    {
        protected readonly PagedDb Db;
        protected abstract RootPage Root { get; }

        private bool _disposed;

        private readonly Context _ctx;

        /// <summary>
        /// A pool of pages that are abandoned during this batch.
        /// </summary>
        private readonly List<DbAddress> _abandoned;

        /// <summary>
        /// The set of pages written during this batch.
        /// </summary>
        private readonly HashSet<DbAddress> _written;

        protected ICollection<DbAddress> Written => _written;

        /// <summary>
        /// Pages that can be reused immediately.
        /// </summary>
        private readonly Stack<DbAddress> _reusedImmediately;

        protected readonly BatchMetrics Metrics;

        protected abstract uint ReusePagesOlderThanBatchId { get; }

        protected BatchBase(PagedDb db)
        {
            Db = db;

            _ctx = Context.Rent();
            _abandoned = _ctx.Abandoned;
            _written = _ctx.Written;
            _reusedImmediately = _ctx.ReusedImmediately;

            IdCache = _ctx.IdCache;

            Metrics = new BatchMetrics();
        }

        public Metadata Metadata => Root.Data.Metadata;

        public bool TryGet(scoped in Key key, out ReadOnlySpan<byte> result)
        {
            if (_disposed)
            {
                ThrowDisposed();
            }

            Metrics.Reads++;

            return Root.TryGet(key, this, out result);

            [DoesNotReturn]
            [StackTraceHidden]
            static void ThrowDisposed()
            {
                throw new ObjectDisposedException("The readonly batch has already been disposed");
            }
        }

        public void SetMetadata(uint blockNumber, in Keccak blockHash)
        {
            Root.Data.Metadata = new Metadata(blockNumber, blockHash);
        }

        public void SetRaw(in Key key, ReadOnlySpan<byte> rawData)
        {
            Metrics.Writes++;
            Root.SetRaw(key, this, rawData);
        }

        public void Destroy(in NibblePath account)
        {
            Metrics.Writes++;
            Root.Destroy(this, account);
        }

        public void DeleteByPrefix(in Key prefix)
        {
            Root.DeleteByPrefix(in prefix, this);
        }

        protected void CheckDisposed()
        {
            if (_disposed)
            {
                ThrowObjectDisposed();
            }

            [DoesNotReturn]
            [StackTraceHidden]
            static void ThrowObjectDisposed()
            {
                throw new ObjectDisposedException("This batch has been disposed already.");
            }
        }

        public override void Prefetch(DbAddress addr) => Db.Prefetch(addr);

        public override Page GetNewPage(out DbAddress addr, bool clear)
        {
            if (_reusedImmediately.TryPop(out addr))
            {
                Debug.Assert(Db._manager.IsValidAddress(addr));
                Metrics.PagesReused++;
            }
            else if (TryGetNoLongerUsedPage(out addr))
            {
                Debug.Assert(Db._manager.IsValidAddress(addr));
                Metrics.PagesReused++;
            }
            else
            {
                Metrics.PagesAllocated++;

                // on failure to reuse a page, default to allocating a new one.
                addr = Root.Data.GetNextFreePage();
                Debug.Assert(Db._manager.IsValidAddress(addr),
                    $"The address of the next free page {addr} breaches the size of the database.");
            }

            var page = GetAtForWriting(addr);

            // if (reused)
            // {
            //     Debug.Assert(page.Header.PageType == PageType.Abandoned || page.Header.BatchId <= BatchId - _db._historyDepth,
            //         $"The page at {addr} is reused at batch {BatchId} even though it was recently written at {page.Header.BatchId}. " +
            //         $"Only {nameof(PageType.Abandoned)} can be reused in that manner.");
            // }

            if (clear)
            {
                page.Clear();
            }

            _written.Add(addr);

            // Clear whole header first
            page.Header = default;

            AssignBatchId(page);
            return page;
        }

        /// <summary>
        /// Stores the abandoned in the root.
        /// </summary>
        protected int MemoizeAbandoned()
        {
            if (_reusedImmediately.Count > 0)
            {
                _abandoned.AddRange(_reusedImmediately);
                _reusedImmediately.Clear();
            }

            if (_abandoned.Count == 0)
            {
                // nothing to memoize
                return 0;
            }

            Root.Data.AbandonedList.Register(_abandoned, this);

            return _abandoned.Count;
        }

        private bool TryGetNoLongerUsedPage(out DbAddress found)
        {
            var claimed = Root.Data.AbandonedList.TryGet(out found, ReusePagesOlderThanBatchId, this);

#if TRACKING_REUSED_PAGES
            if (claimed)
            {
                if (_db._registeredForReuse.Remove(found) == false)
                {
                    throw new Exception(
                        $"The page {found} is not registered as reusable. Must have been taken before. It's tried to be reused again at batch {BatchId}.");
                }
            }
#endif

            return claimed;
        }

        private void RegisterForFutureReuse(DbAddress addr)
        {
#if TRACKING_REUSED_PAGES
            // register at this batch
            ref var batchId =
 ref CollectionsMarshal.GetValueRefOrAddDefault(_db._registeredForReuse, addr, out var exists);
            if (exists)
            {
                throw new Exception(
                    $"The page {addr} that is tried to be registered for reuse at batch {BatchId} has already been registered at batch {batchId}");
            }

            batchId = BatchId;
#endif

            _abandoned.Add(addr);
        }

        public override void RegisterForFutureReuse(Page page, bool possibleImmediateReuse = false)
        {
            var addr = GetAddress(page);

            if (page.Header.BatchId == BatchId)
            {
                if (possibleImmediateReuse)
                {
                    _reusedImmediately.Push(addr);
                    return;
                }

                Metrics.RegisteredToReuseAfterWritingThisBatch++;
            }

            RegisterForFutureReuse(addr);
        }

        public override void NoticeAbandonedPageReused(Page page)
        {
#if TRACKING_REUSED_PAGES
            var addr = _db.GetAddress(page);
            if (_db._registeredForReuse.Remove(addr) == false)
            {
                throw new Exception($"The page {addr} should have been registered as registered for the reuse but it has not.");
            }
#endif
        }

        public override IDictionary<Keccak, uint> IdCache { get; }

        public void Dispose()
        {
            CheckDisposed();
            _disposed = true;
            Context.Return(_ctx);

            DisposeImpl();
        }

        /// <summary>
        /// Clears the transient state.
        /// </summary>
        protected void Clear()
        {
            _ctx.Clear();
            Metrics.Clear();
        }

        protected abstract void DisposeImpl();

        /// <summary>
        /// A reusable context for the <see cref="BatchBase"/>.
        /// </summary>
        private sealed class Context
        {
            private static Context? _shared;

            public static Context Rent() => Interlocked.Exchange(ref _shared, null) ?? new Context();

            public static void Return(Context ctx)
            {
                ctx.Clear();
                Interlocked.CompareExchange(ref _shared, ctx, null);
            }

            private Context()
            {
                Abandoned = new List<DbAddress>();
                Written = new HashSet<DbAddress>();
                IdCache = new ConcurrentDictionary<Keccak, uint>();
                ReusedImmediately = new Stack<DbAddress>();
            }

            public ConcurrentDictionary<Keccak, uint> IdCache { get; }

            public List<DbAddress> Abandoned { get; }
            public HashSet<DbAddress> Written { get; }

            public Stack<DbAddress> ReusedImmediately { get; }

            public void Clear()
            {
                Abandoned.Clear();
                Written.Clear();
                IdCache.Clear();
                ReusedImmediately.Clear();
            }
        }
    }


    public void Flush() => _manager.Flush();

    public void ForceFlush() => _manager.ForceFlush();
}

internal class MissingPagesVisitor : IPageVisitor, IDisposable
{
    private readonly DbAddressSet _pages;

    public static void VerifyNoPagesMissing(RootPage root, PagedDb db, IReadOnlyBatchContext context)
    {
        using var missing = new MissingPagesVisitor(root, db.HistoryDepth);
        root.Accept(missing, db);
        missing.EnsureNoMissing(context);
    }

    private MissingPagesVisitor(RootPage page, int historyDepth)
    {
        _pages = new(page.Data.NextFreePage);

        // Mark all roots
        for (uint i = 0; i < historyDepth; i++)
        {
            Mark(DbAddress.Page(i));
        }
    }

    public IDisposable On<TPage>(scoped ref NibblePath.Builder prefix, TPage page, DbAddress addr)
        where TPage : unmanaged, IPage
    {
        if (typeof(TPage) == typeof(AbandonedPage))
        {
            return On(As<TPage, AbandonedPage>(page), addr);
        }

        return Mark(addr);
    }

    public IDisposable On<TPage>(TPage page, DbAddress addr) where TPage : unmanaged, IPage
    {
        if (typeof(TPage) == typeof(AbandonedPage))
        {
            return On(As<TPage, AbandonedPage>(page), addr);
        }

        return Mark(addr);
    }

    public IDisposable Scope(string name) => NoopDisposable.Instance;

    private static TDestinationPage As<TPage, TDestinationPage>(in TPage page)
        where TDestinationPage : IPage
    {
        return Unsafe.As<TPage, TDestinationPage>(ref Unsafe.AsRef(in page));
    }

    public IDisposable On(AbandonedPage page, DbAddress addr)
    {
        foreach (var abandoned in page.Enumerate())
        {
            Mark(abandoned);
        }

        return Mark(addr);
    }

    private IDisposable Mark(DbAddress addr)
    {
        _pages[addr] = false;
        return NoopDisposable.Instance;
    }

    public void Dispose() => _pages.Dispose();

    public void EnsureNoMissing(IReadOnlyBatchContext batch)
    {
        foreach (var addr in _pages.EnumerateSet())
        {
            var page = batch.GetAt(addr);
            throw new Exception(
                $"The page at {addr} is not reachable from the tree nor from the set of abandoned pages. " +
                $"Highly likely it's a leak. The page is {page.Header.PageType} and was written last in batch {page.Header.BatchId} " +
                $"while the current batch is {batch.BatchId}");
        }
    }
}

public interface IVisitableReadOnlyBatch : IReadOnlyBatch, IVisitable
{
}
