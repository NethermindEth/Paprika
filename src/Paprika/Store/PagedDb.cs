using System.Buffers.Binary;
using System.Diagnostics;
using System.Diagnostics.Metrics;
using System.Runtime.InteropServices;
using Paprika.Chain;
using Paprika.Crypto;
using Paprika.Data;
using Paprika.Store.PageManagers;
using Paprika.Utils;

namespace Paprika.Store;

/// <summary>
/// The base class for page db implementations.
/// </summary>
/// <remarks>
/// Assumes a continuous memory allocation as it provides addressing based on the pointers.
/// </remarks>
public class PagedDb : IPageResolver, IDb, IDisposable
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
    private readonly MetricsExtensions.IAtomicIntGauge _dbSize;

    // pooled objects
    private Context? _ctx;
    private readonly BufferPool _pooledRoots;

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
        _pooledRoots = new BufferPool(16, true, "PagedDb-Roots");

        RootInit();

        _meter = new Meter("Paprika.Store.PagedDb");
        _dbSize = _meter.CreateAtomicObservableGauge("DB Size", "MB", "The size of the database in MB");

        _reads = _meter.CreateCounter<long>("Reads", "Reads", "The number of reads db handles");
        _writes = _meter.CreateCounter<long>("Writes", "Writes", "The number of writes db handles");
        _commits = _meter.CreateCounter<long>("Commits", "Commits", "The number of batch commits db handles");
        _commitDuration =
            _meter.CreateHistogram<float>("Commit duration", "ms", "The time it takes to perform a commit");
        _commitPageCountTotal = _meter.CreateHistogram<int>("Commit page count (total)", "pages",
            "The number of pages flushed during the commit");
        _commitPageCountReused = _meter.CreateHistogram<int>("Commit page count (reused)", "pages",
            "The number of pages flushed during the commit");
        _commitPageCountNewlyAllocated = _meter.CreateHistogram<int>("Commit page count (new)", "pages",
            "The number of pages flushed during the commit");
    }

    public static PagedDb NativeMemoryDb(long size, byte historyDepth = 2) =>
        new(new NativeMemoryPageManager(size, historyDepth), historyDepth);

    public static PagedDb MemoryMappedDb(long size, byte historyDepth, string directory, bool flushToDisk = true) =>
        new(
            new MemoryMappedPageManager(size, historyDepth, directory,
                flushToDisk ? PersistenceOptions.FlushFile : PersistenceOptions.MMapOnly), historyDepth);

    private void ReportRead(long number = 1) => _reads.Add(number);
    private void ReportWrite() => _writes.Add(1);

    private void ReportCommit(TimeSpan elapsed)
    {
        _commits.Add(1);
        _commitDuration.Record((float)elapsed.TotalMilliseconds);
    }

    private void ReportDbSize(int megabytes) => _dbSize.Set(megabytes);

    private void ReportPageCountPerCommit(int totalPageCount, int reused, int newlyAllocated)
    {
        _commitPageCountTotal.Record(totalPageCount);
        _commitPageCountReused.Record(reused);
        _commitPageCountNewlyAllocated.Record(newlyAllocated);
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

    public IReportingReadOnlyBatch BeginReadOnlyBatch(string name = "")
    {
        lock (_batchLock)
        {
            var root = Root;
            return BeginReadOnlyBatch(name, root);
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

    public void Accept(IPageVisitor visitor)
    {
        var i = 0U;

        foreach (var root in _roots)
        {
            visitor.On(root, DbAddress.Page(i++));

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
                throw new Exception("There is another batch active at the moment. Commit the other first");
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
    }

    private DbAddress GetAddress(in Page page) => _manager.GetAddress(page);

    public Page GetAt(DbAddress address) => _manager.GetAt(address);

    private Page GetAtForWriting(DbAddress address, bool reused) => _manager.GetAtForWriting(address, reused);

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


    private class ReadOnlyBatch(PagedDb db, RootPage root, string name) : IReportingReadOnlyBatch, IReadOnlyBatchContext
    {
        public RootPage Root => root;

        private long _reads;
        private volatile bool _disposed;

        public void Dispose()
        {
            db.ReportRead(Volatile.Read(ref _reads));
            _disposed = true;
            db.DisposeReadOnlyBatch(this);
        }

        public Metadata Metadata => root.Data.Metadata;

        public bool TryGet(scoped in Key key, out ReadOnlySpan<byte> result)
        {
            if (_disposed)
                throw new ObjectDisposedException("The readonly batch has already been disposed");

            Interlocked.Increment(ref _reads);

            return root.TryGet(key, this, out result);
        }

        public void Report(IReporter state, IReporter storage)
        {
            if (root.Data.StateRoot.IsNull == false)
            {
                new DataPage(GetAt(root.Data.StateRoot)).Report(state, this, 1);
            }

            if (root.Data.StorageRoot.IsNull == false)
            {
                new FanOutPage(GetAt(root.Data.StorageRoot)).Report(storage, this, 1);
            }
        }

        public uint BatchId => root.Header.BatchId;

        public Page GetAt(DbAddress address) => db._manager.GetAt(address);

        public override string ToString() => $"{nameof(ReadOnlyBatch)}, Name: {name}, BatchId: {BatchId}";
    }

    class Batch : BatchContextBase, IBatch
    {
        private readonly PagedDb _db;
        private readonly RootPage _root;
        private readonly uint _reusePagesOlderThanBatchId;

        private bool _disposed;

        private readonly Context _ctx;

        /// <summary>
        /// A pool of pages that are abandoned during this batch.
        /// </summary>
        private readonly List<DbAddress> _abandoned;

#if DEBUG
        private readonly HashSet<DbAddress> _check = new();
#endif

        /// <summary>
        /// The set of pages written during this batch.
        /// </summary>
        private readonly HashSet<DbAddress> _written;

        private readonly BatchMetrics _metrics;

        public Batch(PagedDb db, RootPage root, uint reusePagesOlderThanBatchId, Context ctx) : base(
            root.Header.BatchId)
        {
            _db = db;
            _root = root;
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
                throw new ObjectDisposedException("The readonly batch has already been disposed");

            _db.ReportRead();

            return _root.TryGet(key, this, out result);
        }

        public void SetMetadata(uint blockNumber, in Keccak blockHash)
        {
            _root.Data.Metadata = new Metadata(blockNumber, blockHash);
        }

        public void SetRaw(in Key key, ReadOnlySpan<byte> rawData)
        {
            _db.ReportWrite();

            _root.SetRaw(key, this, rawData);
        }

        private void SetAtRoot<TPage>(in NibblePath path, in ReadOnlySpan<byte> rawData, ref DbAddress root)
            where TPage : struct, IPageWithData<TPage>
        {
            var data = TryGetPageAlloc(ref root, PageType.Standard);
            var updated = TPage.Wrap(data).Set(path, rawData, this);
            root = _db.GetAddress(updated);
        }

        public void Destroy(in NibblePath account)
        {
            _db.ReportWrite();
            _root.Destroy(this, account);
        }

        private void CheckDisposed()
        {
            if (_disposed)
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
            MemoizeAbandoned();

            _db.ReportPageCountPerCommit(_written.Count, _metrics.PagesReused, _metrics.PagesAllocated);

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

        [DebuggerStepThrough]
        public override Page GetAt(DbAddress address)
        {
            // Getting a page beyond root!
            var nextFree = _root.Data.NextFreePage;
            Debug.Assert(address < nextFree, $"Breached the next free page, NextFree: {nextFree}, retrieved {address}");
            var page = _db.GetAt(address);
            return page;
        }

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
            if (clear)
            {
                page.Clear();
            }

            _written.Add(addr);

            AssignBatchId(page);
            return page;
        }

        private void MemoizeAbandoned()
        {
            if (_abandoned.Count == 0)
            {
                // nothing to memoize
                return;
            }

            _root.Data.AbandonedList.Register(_abandoned, this);
        }

        private bool TryGetNoLongerUsedPage(out DbAddress found)
        {
            return _root.Data.AbandonedList.TryGet(out found, _reusePagesOlderThanBatchId, this);
        }

        public override bool WasWritten(DbAddress addr) => _written.Contains(addr);

        public override void RegisterForFutureReuse(Page page)
        {
            var addr = _db.GetAddress(page);

            
            if (addr.Raw == 298 && BatchId == 6)
            {
                Debugger.Break();
            }
            
#if DEBUG
            Debug.Assert(_check.Add(addr),
                    $"The page {addr} is getting registered second time as abandoned at batch {BatchId}");
#endif

            _abandoned.Add(addr);
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

    private static unsafe Page AllocateOnePage() =>
        new((byte*)NativeMemory.AlignedAlloc(Page.PageSize, (UIntPtr)UIntPtr.Size));

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
        }

        public Dictionary<Keccak, uint> IdCache { get; }

        public Page Page { get; }

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

public interface IReportingReadOnlyBatch : IReporting, IReadOnlyBatch
{
}