//#define TRACKING_REUSED_PAGES

using NonBlocking;
using System.Diagnostics;
using System.Diagnostics.Metrics;
using System.Runtime.InteropServices;
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
public sealed class PagedDb : IPageResolver, IDb, IDisposable
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
    private Batch? _batchCurrent;

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

    // pooled objects
    private Context? _ctx;
    private readonly BufferPool _pooledRoots;

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
        _ctx = new Context();

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

#if TRACKING_REUSED_PAGES
        // Reuse tracking
        _reusablePages = _meter.CreateAtomicObservableGauge("Pages registered for reuse", "count",
            "The number of pages registered to be reused");
#endif
        // Pool
        _pooledRoots = new BufferPool(16, true, _meter);
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

        if (_roots[0].Data.NextFreePage < _historyDepth)
        {
            // the 0th page will have the properly number set to first free page
            _roots[0].Data.NextFreePage = DbAddress.Page(_historyDepth);
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
        _pooledRoots.Dispose();
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
        var copy = new RootPage(_pooledRoots.Rent(false));

        root.CopyTo(copy);

        var batch = new ReadOnlyBatch(this, copy, name);
        _batchesReadOnly.Add(batch);
        return batch;
    }

    public IReadOnlyBatch BeginReadOnlyBatch(in Keccak stateHash, string name = "")
    {
        lock (_batchLock)
        {
            for (var back = 0; back < _historyDepth; back++)
            {
                if (_lastRoot - back < 0)
                {
                    break;
                }

                var at = (_lastRoot - back) % _historyDepth;
                ref readonly var root = ref _roots[at];

                if (root.Data.Metadata.StateHash == stateHash)
                {
                    return BeginReadOnlyBatch(name, root);
                }
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
            for (var back = 0; back < _historyDepth; back++)
            {
                if (_lastRoot - back < 0)
                {
                    break;
                }

                var at = (_lastRoot - back) % _historyDepth;
                ref readonly var root = ref _roots[at];

                if (root.Data.Metadata.StateHash == stateHash)
                {
                    return BeginReadOnlyBatch(name, root);
                }
            }

            return BeginReadOnlyBatch(name, Root);
        }
    }

    public IReadOnlyBatch[] SnapshotAll()
    {
        var batches = new List<IReadOnlyBatch>();

        lock (_batchLock)
        {
            for (var back = 0; back < _historyDepth; back++)
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
            for (var back = 0; back < _historyDepth; back++)
            {
                if (_lastRoot - back < 0)
                {
                    break;
                }

                var at = (_lastRoot - back) % _historyDepth;
                ref readonly var root = ref _roots[at];

                if (root.Data.Metadata.StateHash == stateHash)
                {
                    return true;
                }
            }
        }

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
            _pooledRoots.Return(batch.Root.AsPage());
        }
    }

    private IBatch BuildFromRoot(RootPage rootPage)
    {
        lock (_batchLock)
        {
            if (_batchCurrent != null)
            {
                ThrowOnlyOneBatch();
            }

            var ctx = _ctx ?? new Context();
            _ctx = null;

            // prepare root
            var root = new RootPage(ctx.Page);
            rootPage.CopyTo(root);

            // always inc the batchId
            root.Header.BatchId++;

            // select min batch across the one respecting history and the min of all the read-only batches
            var rootBatchId = root.Header.BatchId;

            var minBatch = rootBatchId < _historyDepth ? 0 : rootBatchId - _historyDepth;
            foreach (var batch in _batchesReadOnly)
            {
                minBatch = Math.Min(batch.BatchId, minBatch);
            }

            return _batchCurrent = new Batch(this, root, minBatch, ctx);
        }

        [DoesNotReturn]
        [StackTraceHidden]
        static void ThrowOnlyOneBatch()
        {
            throw new Exception("There is another batch active at the moment. Commit the other first");
        }
    }

    private DbAddress GetAddress(in Page page) => _manager.GetAddress(page);

    public Page GetAt(DbAddress address) => _manager.GetAt(address);

    /// <summary>
    /// Sets the new root but does not bump the _lastRoot that should be done in a lock.
    /// </summary>
    private DbAddress SetNewRoot(RootPage root)
    {
        var pageAddress = (_lastRoot + 1) % _historyDepth;

        root.CopyTo(_roots[pageAddress]);
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

    /// <summary>
    /// Represents a batch that is currently considered a head of a list of promised batches.
    /// This is constantly updated so that there's never a moment when the page table needs a full rebuild.
    /// </summary>
    /// <remarks>
    /// The head batch stores all the written pages in a <see cref="_pageTable"/>, a dictionary mapping an address to a page.
    /// This is a squashed version of all the promised batches and the pages that were written in this batch.
    ///
    /// To check whether it's a historical read-only page or a written one, the header can be checked.
    ///
    /// When committing, filter the page table to find pages that have the same batch id as this one. 
    /// </remarks>
    private class HeadTrackingBatch : BatchBase
    {
        private readonly BufferPool _pool;

        private readonly Dictionary<DbAddress, Page> _pageTable = new();
        private readonly Dictionary<Page, DbAddress> _pageTableReversed = new();
        private readonly uint _reusePagesOlderThanBatchId;

        public HeadTrackingBatch(PagedDb db, RootPage root, uint reusePagesOlderThanBatchId, Context ctx,
            BufferPool pool) : base(db, ctx)
        {
            Root = root;

            _pool = pool;
            _reusePagesOlderThanBatchId = reusePagesOlderThanBatchId;
        }

        public override void Prefetch(DbAddress addr)
        {
        }

        public override uint BatchId { get; }

        protected override RootPage Root { get; }

        protected override uint ReusePagesOlderThanBatchId => _reusePagesOlderThanBatchId;

        public override DbAddress GetAddress(Page page) =>
            _pageTableReversed.TryGetValue(page, out var addr) ? addr : Db.GetAddress(page);

        protected override Page GetAtImpl(DbAddress addr, bool write)
        {
            ref var page = ref CollectionsMarshal.GetValueRefOrNullRef(_pageTable, addr);

            if (Unsafe.IsNullRef(ref page) == false)
            {
                // The value exists
                var writtenThisBatch = page.Header.BatchId == BatchId;

                if (!write || writtenThisBatch)
                {
                    return page;
                }

                // Not written this batch, allocate and copy. Memoize in the slot
                page = MakeCopy(addr, page);
                return page;
            }

            // Does not exist, fetch from db
            var fromDb = Db.GetAt(addr);

            // Make copy on write, while return raw from db if a read.
            if (!write)
            {
                return fromDb;
            }

            // The entry did not exist before, create one
            var copy = MakeCopy(addr, fromDb);
            _pageTable[addr] = page;
            return copy;
        }

        private Page MakeCopy(DbAddress at, Page source)
        {
            var slot = _pool.Rent(false);
            source.CopyTo(slot);

            // Remember reversed mapping
            _pageTableReversed[source] = at;
            return slot;
        }
    }

    private sealed class Batch(PagedDb db, RootPage root, uint reusePagesOlderThanBatchId, Context ctx)
        : BatchBase(db, ctx), IBatch
    {
        private bool _verify;

        protected override RootPage Root { get; } = root;

        protected override uint ReusePagesOlderThanBatchId { get; } = reusePagesOlderThanBatchId;
        protected override Page GetAtImpl(DbAddress addr, bool write) => Db.GetAt(addr);

        public override DbAddress GetAddress(Page page) => Db.GetAddress(page);

        public void VerifyDbPagesOnCommit()
        {
            _verify = true;
        }

        public void VerifyNoPagesMissing() => MissingPagesVisitor.VerifyNoPagesMissing(Root, Db, this);

        public async ValueTask Commit(CommitOptions options)
        {
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
                Debug.Assert(ReferenceEquals(this, Db._batchCurrent));
                Db._batchCurrent = null;
            }

            Db.ReportCommit(watch.Elapsed);
        }

        public override uint BatchId { get; } = root.Header.BatchId;
    }

    /// <summary>
    /// A base class for any <see cref="IBatch"/>-like object.
    /// </summary>
    abstract class BatchBase : BatchContextBase, IDataSetter
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

        protected BatchBase(PagedDb db, Context ctx)
        {
            Db = db;

            _ctx = ctx;
            _abandoned = ctx.Abandoned;
            _written = ctx.Written;
            _reusedImmediately = ctx.ReusedImmediately;

            IdCache = ctx.IdCache;

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

        [DebuggerStepThrough]
        public sealed override Page GetAt(DbAddress address)
        {
            // Getting a page beyond root!
            var nextFree = Root.Data.NextFreePage;
            Debug.Assert(address < nextFree, $"Breached the next free page, NextFree: {nextFree}, retrieved {address}");

            return GetAtImpl(address, false);
        }

        protected abstract Page GetAtImpl(DbAddress addr, bool write);

        public override void Prefetch(DbAddress addr) => Db.Prefetch(addr);

        public override Page GetNewPage(out DbAddress addr, bool clear)
        {
            if (_reusedImmediately.TryPop(out addr))
            {
                Metrics.PagesReused++;
            }
            else if (TryGetNoLongerUsedPage(out addr))
            {
                Metrics.PagesReused++;
            }
            else
            {
                Metrics.PagesAllocated++;

                // on failure to reuse a page, default to allocating a new one.
                addr = Root.Data.GetNextFreePage();
            }

            var page = GetAtImpl(addr, true);

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
            var addr = Db.GetAddress(page);

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

        public override Dictionary<Keccak, uint> IdCache { get; }

        public void Dispose()
        {
            _disposed = true;

            lock (Db._batchLock)
            {
                if (ReferenceEquals(Db._batchCurrent, this))
                {
                    Db._batchCurrent = null;
                }

                // clear and return
                _ctx.Clear();
                Db._ctx = _ctx;
            }
        }
    }

    /// <summary>
    /// A reusable context for the write batch.
    /// </summary>
    private sealed class Context
    {
        public unsafe Context()
        {
            Page = new((byte*)NativeMemory.AlignedAlloc(Page.PageSize, (UIntPtr)UIntPtr.Size));
            Abandoned = new List<DbAddress>();
            Written = new HashSet<DbAddress>();
            IdCache = new Dictionary<Keccak, uint>();
            ReusedImmediately = new Stack<DbAddress>();
        }

        public Dictionary<Keccak, uint> IdCache { get; }

        public Page Page { get; }

        public List<DbAddress> Abandoned { get; }
        public HashSet<DbAddress> Written { get; }

        public Stack<DbAddress> ReusedImmediately { get; }

        public void Clear()
        {
            Abandoned.Clear();
            Written.Clear();
            IdCache.Clear();
            Abandoned.Clear();
            ReusedImmediately.Clear();

            // no need to clear, it's always overwritten
            //Page.Clear();
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