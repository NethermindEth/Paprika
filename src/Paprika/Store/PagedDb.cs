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

    /// <summary>
    /// The number of db pages that a single root actually occupies.
    /// The pages, when flushed, should be written from the highest address to the lowest.
    /// </summary>
    public const uint DbPagesPerRoot = 2;

    private uint DataStart => _historyDepth * DbPagesPerRoot;

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
    private readonly MetricsExtensions.IAtomicIntGauge _dbSize;

    // pooled objects
    private Context? _ctx;
    private readonly BufferPool _pooledPages;

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

#if TRACKING_REUSED_PAGES
        // Reuse tracking
        _reusablePages = _meter.CreateAtomicObservableGauge("Pages registered for reuse", "count",
            "The number of pages registered to be reused");
#endif
        // Pool
        _pooledPages = new BufferPool(16, true, _meter);
    }

    public static PagedDb NativeMemoryDb(long size, byte historyDepth = 2) =>
        new(new NativeMemoryPageManager(size, historyDepth), historyDepth);

    public static PagedDb MemoryMappedDb(long size, byte historyDepth, string directory, bool flushToDisk = true) =>
        new(
            new MemoryMappedPageManager(size, historyDepth, directory,
                flushToDisk ? PersistenceOptions.FlushFile : PersistenceOptions.MMapOnly), historyDepth);

    public void Prefetch(DbAddress address) => _manager.Prefetch(address);

    private void ReportReads(long number) => _reads.Add(number);

    private void ReportWrites(long number) => _writes.Add(number);

    private void ReportCommit(TimeSpan elapsed)
    {
        _commits.Add(1);
        _commitDuration.Record((float)elapsed.TotalMilliseconds);
    }

    private void ReportDbSize(int megabytes) => _dbSize.Set(megabytes);

    private void ReportPageCountPerCommit(int totalPageCount, int reused, int newlyAllocated, int abandonedCount)
    {
        _commitPageCountTotal.Record(totalPageCount);
        _commitPageCountReused.Record(reused);
        _commitPageCountNewlyAllocated.Record(newlyAllocated);
        _commitPageAbandoned.Record(abandonedCount);
    }

    private void RootInit()
    {
        // create all root pages for the history depth
        for (uint i = 0; i < _historyDepth; i++)
        {
            _roots[i] = new RootPage(_manager.GetAt(DbAddress.Page(i * DbPagesPerRoot)));
        }

        if (_roots[0].Data.NextFreePage < DataStart)
        {
            // the 0th page will have the properly number set to first free page
            _roots[0].Data.NextFreePage = DbAddress.Page(DataStart);
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
        _pooledPages.Dispose();
        _manager.Dispose();
        _meter.Dispose();
    }

    /// <summary>
    /// Begins a batch representing the next block.
    /// </summary>
    /// <returns></returns>
    public IBatch BeginNextBatch() => BuildFromRoot(Root);

    IReadOnlyBatch IDb.BeginReadOnlyBatch(string name) => BeginReadOnlyBatch(name);

    public IReportingReadOnlyBatch BeginReadOnlyBatch(string name = "")
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
        // Root
        var rootCopy = new RootPage(_pooledPages.Rent(false));
        root.CopyTo(rootCopy);

        // Abandoned
        var abandonedCopy = _pooledPages.Rent(false);
        GetAt(GetAddress(root.AsPage()).Next).CopyTo(abandonedCopy);

        var batch = new ReadOnlyBatch(this, rootCopy, abandonedCopy, name);
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
            var addr = DbAddress.Page(i);
            i += DbPagesPerRoot;

            using (visitor.On(root, addr))
            {
                root.Accept(visitor, this, GetAt(addr.Next));
            }
        }
    }

    public void VisitRoot(IPageVisitor visitor)
    {
        var root = Root;
        var addr = GetAddress(Root.AsPage());

        using (visitor.On(root, addr))
        {
            root.Accept(visitor, this, GetAt(addr.Next));
        }
    }

    /// <summary>
    /// Allows to walk through the pages. No locks, no safety, use for dev purposes.
    /// </summary>
    /// <returns></returns>
    public IEnumerable<Page> UnsafeEnumerateNonRoot()
    {
        for (uint i = DataStart; i < Root.Data.NextFreePage; i++)
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
        }

        _pooledPages.Return(batch.Root.AsPage());
        _pooledPages.Return(batch.Abandoned.AsPage());
    }

    private IBatch BuildFromRoot(RootPage rootPage)
    {
        var ctx = Interlocked.Exchange(ref _ctx, null) ?? new Context();

        // prepare root
        var root = new RootPage(ctx.PageForRoot);
        rootPage.CopyTo(root);

        // always inc the batchId
        root.Header.BatchId++;

        // copy abandoned
        var abandoned = ctx.PageForAbandoned;
        GetAt(GetAddress(rootPage.AsPage()).Next).CopyTo(abandoned);

        // copying done above, to minimize the lock
        lock (_batchLock)
        {
            if (_batchCurrent != null)
            {
                ThrowOnlyOneBatch();
            }

            // select min batch across the one respecting history and the min of all the read-only batches
            var rootBatchId = root.Header.BatchId;

            var minBatch = rootBatchId < _historyDepth ? 0 : rootBatchId - _historyDepth;
            foreach (var batch in _batchesReadOnly)
            {
                minBatch = Math.Min(batch.BatchId, minBatch);
            }

            return _batchCurrent = new Batch(this, root, abandoned, minBatch, ctx);
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

    private Page GetAtForWriting(DbAddress address, bool reused) => _manager.GetAtForWriting(address, reused);

    /// <summary>
    /// Sets the new root but does not bump the _lastRoot that should be done in a lock.
    /// </summary>
    private DbAddress SetNewRoot(RootPage root)
    {
        var next = NextRootIndex;
        root.CopyTo(_roots[next]);
        return DbAddress.Page(next * DbPagesPerRoot);
    }

    private uint NextRootIndex => (uint)(_lastRoot + 1) % _historyDepth;

    private void CommitNewRoot() => _lastRoot += 1;


    private sealed class ReadOnlyBatch(PagedDb db, RootPage root, Page abandoned, string name)
        : IReportingReadOnlyBatch, IReadOnlyBatchContext
    {
        [ThreadStatic] private static ConcurrentDictionary<Keccak, uint>? s_cache;

        private ConcurrentDictionary<Keccak, uint> _idCache = Interlocked.Exchange(ref s_cache, null) ?? new(
            Environment.ProcessorCount,
            RootPage.IdCacheLimit);

        public RootPage Root => root;
        public Page Abandoned => abandoned;

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

        public void Report(IReporter state, IReporter storage, IReporter ids, out long totalAbandoned)
        {
            ref readonly var data = ref root.Data;

            totalAbandoned = AbandonedList.Wrap(abandoned).GatherTotalAbandoned(this);

            if (data.StateRoot.IsNull == false)
            {
                new Merkle.StateRootPage(GetAt(root.Data.StateRoot)).Report(state, this, 0, 0);
            }

            data.Storage.Report(storage, this, 0, 0);
            data.StorageMerkle.Report(storage, this, 0, 0);
            data.Ids.Report(ids, this, 0, 0);
        }

        public uint BatchId => root.Header.BatchId;

        public IDictionary<Keccak, uint> IdCache
        {
            get
            {
                ObjectDisposedException.ThrowIf(_disposed, this);
                return _idCache;
            }
        }

        public void Prefetch(DbAddress address) => db.Prefetch(address);

        public Page GetAt(DbAddress address) => db._manager.GetAt(address);

        public override string ToString() => $"{nameof(ReadOnlyBatch)}, Name: {name}, BatchId: {BatchId}";
    }

    class Batch : BatchContextBase, IBatch
    {
        private readonly PagedDb _db;
        private readonly RootPage _root;
        private readonly Page _abandonedPage;
        private readonly uint _reusePagesOlderThanBatchId;
        private bool _verify;
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

        private readonly BatchMetrics _metrics;


        public Batch(PagedDb db, RootPage root, Page abandoned, uint reusePagesOlderThanBatchId, Context ctx) : base(
            root.Header.BatchId)
        {
            _db = db;
            _root = root;
            _abandonedPage = abandoned;
            _reusePagesOlderThanBatchId = reusePagesOlderThanBatchId;
            _ctx = ctx;
            _abandoned = ctx.Abandoned;
            _written = ctx.Written;

            IdCache = ctx.IdCache;

            _metrics = new BatchMetrics();
        }

        public Metadata Metadata => _root.Data.Metadata;

        public bool TryGet(scoped in Key key, out ReadOnlySpan<byte> result)
        {
            if (_disposed)
            {
                ThrowDisposed();
            }

            _metrics.Reads++;

            return _root.TryGet(key, this, out result);

            [DoesNotReturn]
            [StackTraceHidden]
            static void ThrowDisposed()
            {
                throw new ObjectDisposedException("The readonly batch has already been disposed");
            }
        }

        public void SetMetadata(uint blockNumber, in Keccak blockHash)
        {
            _root.Data.Metadata = new Metadata(blockNumber, blockHash);
        }

        public void SetRaw(in Key key, ReadOnlySpan<byte> rawData)
        {
            _metrics.Writes++;

            _root.SetRaw(key, this, rawData);
        }

        public void Destroy(in NibblePath account)
        {
            _metrics.Writes++;
            _root.Destroy(this, account);
        }

        private void CheckDisposed()
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

        public void Report(IReporter reporter)
        {
            throw new NotImplementedException();
        }

        public async ValueTask Commit(CommitOptions options)
        {
            var watch = Stopwatch.StartNew();

            CheckDisposed();

            // memoize the abandoned so that it's preserved for future uses
            MemoizeAbandoned(_db.NextRootIndex);

            if (_verify)
            {
                using var missing = new MissingPagesVisitor(_root, _db._historyDepth);
                _root.Accept(missing, this, _abandonedPage);
                missing.EnsureNoMissing(this);
            }

            // report metrics
            _db.ReportPageCountPerCommit(_written.Count, _metrics.PagesReused, _metrics.PagesAllocated,
                _abandoned.Count);

            _db.ReportReads(_metrics.Reads);
            _db.ReportWrites(_metrics.Writes);

#if TRACKING_REUSED_PAGES
            _db._reusablePages.Set(_db._registeredForReuse.Count);
#endif

            await _db._manager.FlushPages(_written, options);

            var newRootPage = _db.SetNewRoot(_root);

            // report
            _db.ReportDbSize(GetRootSizeInMb(_root));

            await _db._manager.FlushRootPage(newRootPage, options);

            lock (_db._batchLock)
            {
                _db.CommitNewRoot();
                Debug.Assert(ReferenceEquals(this, _db._batchCurrent));
                _db._batchCurrent = null;
            }

            _db.ReportCommit(watch.Elapsed);
        }

        IBatchStats? IBatch.Stats => Stats;

        public void VerifyDbPagesOnCommit()
        {
            _verify = true;
        }

        [DebuggerStepThrough]
        public override Page GetAt(DbAddress address)
        {
            // Getting a page beyond root!
            var nextFree = _root.Data.NextFreePage;
            Debug.Assert(address < nextFree, $"Breached the next free page, NextFree: {nextFree}, retrieved {address}");
            var page = _db.GetAt(address);
            return page;
        }

        public override void Prefetch(DbAddress address) => _db.Prefetch(address);

        public override DbAddress GetAddress(Page page) => _db.GetAddress(page);

        public override Page GetNewPage(out DbAddress addr, bool clear)
        {
            bool reused;
            if (TryGetNoLongerUsedPage(out addr))
            {
                reused = true;
                _metrics.ReportPageReused();
            }
            else
            {
                reused = false;
                _metrics.ReportNewPageAllocation();

                // on failure to reuse a page, default to allocating a new one.
                addr = _root.Data.GetNextFreePage();
            }

            var page = _db.GetAtForWriting(addr, reused);

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

            AssignBatchId(page);
            return page;
        }

        private void MemoizeAbandoned(uint nextRootIndex)
        {
            ref var list = ref AbandonedList.Wrap(_abandonedPage);

            if (_abandoned.Count > 0)
            {
                list.Register(_abandoned, this);
            }

            var addr = DbAddress.Page(nextRootIndex * DbPagesPerRoot + 1);

            Debug.Assert(addr < _db.NextFreePage, "Abandoned should not breach the range of pages at the beginning of the database");
            Debug.Assert(addr.Raw % DbPagesPerRoot == 1, "Abandoned page should lie next to the root");

            _abandonedPage.CopyTo(GetAt(addr));
            _written.Add(addr);
        }

        private bool TryGetNoLongerUsedPage(out DbAddress found)
        {
            var claimed = AbandonedList.Wrap(_abandonedPage).TryGet(out found, _reusePagesOlderThanBatchId, this);

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

        public override bool WasWritten(DbAddress addr) => _written.Contains(addr);

        public override void RegisterForFutureReuse(Page page)
        {
            var addr = _db.GetAddress(page);

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

            lock (_db._batchLock)
            {
                if (ReferenceEquals(_db._batchCurrent, this))
                {
                    _db._batchCurrent = null;
                }

                // clear and return
                _ctx.Clear();
                _db._ctx = _ctx;
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
            PageForRoot = new((byte*)NativeMemory.AlignedAlloc(Page.PageSize, (UIntPtr)UIntPtr.Size));
            PageForAbandoned = new((byte*)NativeMemory.AlignedAlloc(Page.PageSize, (UIntPtr)UIntPtr.Size));
            Abandoned = new List<DbAddress>();
            Written = new HashSet<DbAddress>();
            IdCache = new Dictionary<Keccak, uint>();
        }

        public Dictionary<Keccak, uint> IdCache { get; }

        public Page PageForRoot { get; }
        public Page PageForAbandoned { get; }

        public List<DbAddress> Abandoned { get; }
        public HashSet<DbAddress> Written { get; }

        public void Clear()
        {
            Abandoned.Clear();
            Written.Clear();
            IdCache.Clear();
            Abandoned.Clear();

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

    public MissingPagesVisitor(RootPage page, byte historyDepth)
    {
        _pages = new(page.Data.NextFreePage);

        // Mark all roots
        for (uint i = 0; i < historyDepth * PagedDb.DbPagesPerRoot; i++)
        {
            Mark(DbAddress.Page(i));
        }
    }

    public IDisposable On(RootPage page, DbAddress addr) => Mark(addr);

    public IDisposable On(AbandonedPage page, DbAddress addr)
    {
        foreach (var abandoned in page.Enumerate())
        {
            Mark(abandoned);
        }

        return Mark(addr);
    }

    public IDisposable On(DataPage page, DbAddress addr) => Mark(addr);

    public IDisposable On(FanOutPage page, DbAddress addr) => Mark(addr);

    public IDisposable On(LeafPage page, DbAddress addr) => Mark(addr);

    public IDisposable On<TNext>(StorageFanOutPage<TNext> page, DbAddress addr)
        where TNext : struct, IPageWithData<TNext>
        => Mark(addr);

    public IDisposable On(LeafOverflowPage page, DbAddress addr) => Mark(addr);

    public IDisposable On(Merkle.StateRootPage data, DbAddress addr) => Mark(addr);

    private IDisposable Mark(DbAddress addr)
    {
        _pages[addr] = false;
        return Disposable.Instance;
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

public interface IReportingReadOnlyBatch : IReporting, IReadOnlyBatch
{
}