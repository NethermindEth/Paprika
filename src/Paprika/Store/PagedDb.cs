using System.Buffers.Binary;
using System.Diagnostics;
using System.Runtime.InteropServices;
using Nethermind.Int256;
using Paprika.Crypto;
using Paprika.Data;
using Paprika.Store.PageManagers;

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
    private readonly Action<IBatchMetrics>? _reporter;
    private long _lastRoot;
    private readonly RootPage[] _roots;

    // batches
    private readonly object _batchLock = new();
    private readonly List<ReadOnlyBatch> _batchesReadOnly = new();
    private Batch? _batchCurrent;

    // pool
    private Context? _ctx;

    /// <summary>
    /// Initializes the paged db.
    /// </summary>
    /// <param name="manager">The page manager.</param>
    /// <param name="historyDepth">The depth history represent how many blocks should be able to be restored from the past. Effectively,
    ///     a reorg depth. At least 2 are required</param>
    /// <param name="reporter"></param>
    private PagedDb(IPageManager manager, byte historyDepth, Action<IBatchMetrics>? reporter)
    {
        if (historyDepth < MinHistoryDepth)
            throw new ArgumentException($"{nameof(historyDepth)} should be bigger than {MinHistoryDepth}");

        _manager = manager;
        _historyDepth = historyDepth;
        _reporter = reporter;
        _roots = new RootPage[historyDepth];
        _batchCurrent = null;
        _ctx = new Context();

        RootInit();
    }

    public static PagedDb NativeMemoryDb(ulong size, byte historyDepth = 2, Action<IBatchMetrics>? reporter = null) =>
        new(new NativeMemoryPageManager(size), historyDepth, reporter);

    public static PagedDb MemoryMappedDb(ulong size, byte historyDepth, string directory,
        Action<IBatchMetrics>? reporter = null) =>
        new(new MemoryMappedPageManager(size, historyDepth, directory), historyDepth, reporter);

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
        for (var i = 0; i < MinHistoryDepth; i++)
        {
            if (_roots[i].Header.BatchId > _lastRoot)
            {
                _lastRoot = i;
            }
        }
    }

    public double Megabytes => (double)(int)Root.Data.NextFreePage * Page.PageSize / 1024 / 1024;

    private RootPage Root => _roots[_lastRoot % _historyDepth];

    public void Dispose()
    {
        _manager.Dispose();
    }

    /// <summary>
    /// Begins a batch representing the next block.
    /// </summary>
    /// <returns></returns>
    public IBatch BeginNextBatch() => BuildFromRoot(Root);

    public IReadOnlyBatch BeginReadOnlyBatch()
    {
        lock (_batchLock)
        {
            var batchId = Root.Header.BatchId;
            var batch = new ReadOnlyBatch(this, batchId, Root.Data.AccountPages.ToArray());
            _batchesReadOnly.Add(batch);
            return batch;
        }
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

    private void DisposeReadOnlyBatch(ReadOnlyBatch batch)
    {
        lock (_batchLock)
        {
            _batchesReadOnly.Remove(batch);
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

    private DbAddress SetNewRoot(RootPage root)
    {
        _lastRoot += 1;
        var pageAddress = _lastRoot % _historyDepth;

        root.CopyTo(_roots[pageAddress]);
        return DbAddress.Page((uint)pageAddress);
    }

    class ReadOnlyBatch : IReadOnlyBatch, IReadOnlyBatchContext
    {
        private readonly PagedDb _db;
        private bool _disposed;

        private readonly DbAddress[] _rootDataPages;

        public ReadOnlyBatch(PagedDb db, uint batchId, DbAddress[] rootDataPages)
        {
            _db = db;
            _rootDataPages = rootDataPages;
            BatchId = batchId;
        }

        public void Dispose()
        {
            _disposed = true;
            _db.DisposeReadOnlyBatch(this);
        }

        public bool TryGet(in Key key, out ReadOnlySpan<byte> result)
        {
            if (_disposed)
                throw new ObjectDisposedException("The readonly batch has already been disposed");

            var addr = RootPage.FindAccountPage(_rootDataPages, key.Path);
            if (addr.IsNull)
            {
                result = default;
                return false;
            }

            return new DataPage(GetAt(addr)).TryGet(key, this, out result);
        }

        public uint BatchId { get; }

        public Page GetAt(DbAddress address) => _db._manager.GetAt(address);
    }

    class Batch : BatchContextBase, IBatch
    {
        private readonly PagedDb _db;
        private readonly RootPage _root;
        private readonly uint _reusePagesOlderThanBatchId;

        private bool _disposed;

        private readonly Context _ctx;

        /// <summary>
        /// A pool of pages that are no longer used and can be reused now.
        /// </summary>
        private readonly Queue<DbAddress> _unusedPool;

        private bool _noUnusedPages;

        /// <summary>
        /// A pool of pages that are abandoned during this batch.
        /// </summary>
        private readonly Queue<DbAddress> _abandoned;

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
            _unusedPool = ctx.Unused;
            _abandoned = ctx.Abandoned;
            _written = ctx.Written;

            _metrics = new BatchMetrics();
        }

        public bool TryGet(in Key key, out ReadOnlySpan<byte> result)
        {
            CheckDisposed();

            var addr = RootPage.FindAccountPage(_root.Data.AccountPages, key.Path);

            if (addr.IsNull)
            {
                result = default;
                return false;
            }

            return new DataPage(GetAt(addr)).TryGet(key, this, out result);
        }

        public void Set(in Keccak key, in Account account)
        {
            ref var addr = ref TryGetPageAlloc(key, out var page);
            var updated = page.SetAccount(GetPath(key), account, this);
            addr = _db.GetAddress(updated);
        }

        public void SetStorage(in Keccak key, in Keccak address, UInt256 value)
        {
            ref var addr = ref TryGetPageAlloc(key, out var page);
            var updated = page.SetStorage(GetPath(key), address, value, this);
            addr = _db.GetAddress(updated);
        }

        private ref DbAddress TryGetPageAlloc(in Keccak key, out DataPage page)
        {
            CheckDisposed();

            ref var addr = ref RootPage.FindAccountPage(_root.Data.AccountPages, key);
            Page p;
            if (addr.IsNull)
            {
                p = GetNewPage(out addr, true);

                p.Header.PageType = PageType.Standard;
                p.Header.TreeLevel = 1; // the root is level 0, start with 1
            }
            else
            {
                p = GetAt(addr);
            }

            page = new DataPage(p);

            return ref addr;
        }

        private void CheckDisposed()
        {
            if (_disposed)
            {
                throw new ObjectDisposedException("This batch has been disposed already.");
            }
        }

        public async ValueTask Commit(CommitOptions options)
        {
            CheckDisposed();

            // memoize the abandoned so that it's preserved for future uses 
            MemoizeAbandoned();

            await _db._manager.FlushPages(_written, options);

            var newRootPage = _db.SetNewRoot(_root);

            await _db._manager.FlushRootPage(newRootPage, options);

            // if reporter passed
            _db._reporter?.Invoke(_metrics);

            lock (_db._batchLock)
            {
                Debug.Assert(ReferenceEquals(this, _db._batchCurrent));
                _db._batchCurrent = null;
            }
        }

        public override Page GetAt(DbAddress address) => _db.GetAt(address);

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
                page.Clear();

            _written.Add(addr);

            AssignBatchId(page);
            return page;
        }

        private void MemoizeAbandoned()
        {
            if (_abandoned.Count == 0 && _unusedPool.Count == 0)
            {
                // nothing to memoize
                return;
            }

            var abandonedPages = _root.Data.AbandonedPages;

            // The current approach is to squash sets of _abandoned and _unused into one set.
            // The disadvantage is that the _unused will get their AbandonedAt bumped to the recent one.
            // This meant that that they will not be reused sooner.
            // The advantage is that usually, there number of abandoned pages create is lower and
            // the book keeping is simpler.

            // The pages are put in a linked list, when first -> ... -> last -> NULL

            var newPage = GetNewPage(out var firstAddr, true);

            newPage.Header.PageType = PageType.Abandoned;
            newPage.Header.TreeLevel = 0;
            newPage.Header.PaprikaVersion = 1;

            var first = new AbandonedPage(newPage)
            {
                AbandonedAtBatch = BatchId
            };
            var last = first;

            // process abandoned
            while (_abandoned.TryDequeue(out var page))
            {
                Debug.Assert(page.IsValidPageAddress, "Only valid pages should be reused");

                first = first.EnqueueAbandoned(this, _db.GetAddress(first.AsPage()), page);
            }

            // process unused
            while (_unusedPool.TryDequeue(out var page))
            {
                first = first.EnqueueAbandoned(this, _db.GetAddress(first.AsPage()), page);
            }

            // remember the abandoned by either finding an empty slot, or chaining it to the highest number there
            var nullAddress = abandonedPages.IndexOf(DbAddress.Null);
            if (nullAddress != -1)
            {
                abandonedPages[nullAddress] = GetAddress(first.AsPage());
            }
            else
            {
                // no empty slot, find the youngest one and chain it at the end of this
                var youngest = new AbandonedPage(_db.GetAt(abandonedPages[0]));
                ref var youngestAddr = ref abandonedPages[0];

                foreach (ref var pageAddr in abandonedPages)
                {
                    var current = new AbandonedPage(_db.GetAt(pageAddr));
                    if (current.AbandonedAtBatch > youngest.AbandonedAtBatch)
                    {
                        youngest = current;
                        youngestAddr = pageAddr;
                    }
                }

                // the youngest contains the youngest abandoned pages, but it's not younger than this batch.
                // but... we can't write previously used pages only the current one, so...
                // link back from this batch to previously decommissioned
                last.Next = youngestAddr;

                // write the abandoned in the youngestAddr
                youngestAddr = _db.GetAddress(first.AsPage());
            }
        }

        private bool TryGetNoLongerUsedPage(out DbAddress found)
        {
            if (_noUnusedPages)
            {
                found = default;
                return false;
            }

            // check whether previous operations allocated the pool of unused pages
            if (_unusedPool.Count == 0)
            {
                _metrics.ReportUnusedPoolFetch();

                var abandonedPages = _root.Data.AbandonedPages;

                // find the page across all the abandoned pages, so that it's the oldest one
                if (TryFindOldest(abandonedPages, out var oldest, out var oldestAddress))
                {
                    // check whether the oldest page qualifies for being the pool
                    if (oldest.AbandonedAtBatch < _reusePagesOlderThanBatchId)
                    {
                        // 1. copy all the pages to the pool
                        while (oldest.TryDequeueFree(out var addr))
                        {
                            _unusedPool.Enqueue(addr);
                        }

                        // 2. register the oldest page for reuse
                        RegisterForFutureReuse(oldest.AsPage());

                        // 3. write its next in place of this page so that it's used as the pool
                        var indexInRoot = abandonedPages.IndexOf(oldestAddress);
                        abandonedPages[indexInRoot] = oldest.Next;
                    }
                    else
                    {
                        // there are no matching sets of unused pages
                        // memoize it so that the follow up won't query over and over again
                        _noUnusedPages = true;
                    }
                }
            }

            return _unusedPool.TryDequeue(out found);
        }

        private bool TryFindOldest(Span<DbAddress> abandonedPages, out AbandonedPage oldest,
            out DbAddress oldestAddress)
        {
            AbandonedPage? currentOldest = default;
            oldestAddress = default;

            foreach (var abandonedPageAddr in abandonedPages)
            {
                if (abandonedPageAddr.IsNull == false)
                {
                    var current = new AbandonedPage(_db.GetAt(abandonedPageAddr));
                    uint oldestBatchId = currentOldest?.AbandonedAtBatch ?? uint.MaxValue;
                    if (current.AbandonedAtBatch < oldestBatchId)
                    {
                        currentOldest = current;
                        oldestAddress = abandonedPageAddr;
                    }
                }
            }

            oldest = currentOldest.GetValueOrDefault();
            return currentOldest != null;
        }

        public override bool WasWritten(DbAddress addr) => _written.Contains(addr);

        protected override void RegisterForFutureReuse(Page page)
        {
            var addr = _db.GetAddress(page);
            _abandoned.Enqueue(addr);
        }

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
            Page = new Page((byte*)NativeMemory.AlignedAlloc((UIntPtr)Page.PageSize, (UIntPtr)UIntPtr.Size));
            Abandoned = new Queue<DbAddress>();
            Unused = new Queue<DbAddress>();
            Written = new HashSet<DbAddress>();
        }

        public Page Page { get; }

        public Queue<DbAddress> Unused { get; }

        public Queue<DbAddress> Abandoned { get; }
        public HashSet<DbAddress> Written { get; }

        public void Clear()
        {
            Unused.Clear();
            Abandoned.Clear();
            Written.Clear();

            // no need to clear, it's always overwritten
            //Page.Clear();
        }
    }

    private static NibblePath GetPath(in Keccak key)
    {
        return NibblePath.FromKey(key).SliceFrom(RootPage.Payload.RootNibbleLevel);
    }
}