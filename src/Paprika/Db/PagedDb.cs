using System.Buffers.Binary;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Paprika.Crypto;
using Paprika.Pages;

namespace Paprika.Db;

/// <summary>
/// The base class for page db implementations.
/// </summary>
/// <remarks>
/// Assumes a continuous memory allocation as it provides addressing based on the pointers.
/// </remarks>
public abstract unsafe class PagedDb : IDb, IDisposable
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

    private const byte RootLevel = 0;

    private readonly byte _historyDepth;
    private readonly int _maxPage;
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
    /// <param name="size">The size of the database, should be a multiple of <see cref="Page.PageSize"/>.</param>
    /// <param name="historyDepth">The depth history represent how many blocks should be able to be restored from the past. Effectively,
    /// a reorg depth. At least 2 are required</param>
    protected PagedDb(ulong size, byte historyDepth)
    {
        if (historyDepth < MinHistoryDepth)
            throw new ArgumentException($"{nameof(historyDepth)} should be bigger than {MinHistoryDepth}");

        _historyDepth = historyDepth;
        _maxPage = (int)(size / Page.PageSize);
        _roots = new RootPage[MinHistoryDepth];
        _batchCurrent = null;
        _ctx = new Context();
    }

    protected void RootInit()
    {
        // create all root pages for the history depth
        for (uint i = 0; i < _historyDepth; i++)
        {
            _roots[i] = new RootPage(GetAt(DbAddress.Page(i)));
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

    protected abstract void* Ptr { get; }

    public double TotalUsedPages => ((double)(int)Root.Data.NextFreePage) / _maxPage;

    public double ActualMegabytesOnDisk => (double)(int)Root.Data.NextFreePage * Page.PageSize / 1024 / 1024;

    private RootPage Root => _roots[_lastRoot % _historyDepth];

    private Page GetAt(DbAddress address)
    {
        Debug.Assert(address.IsValidPageAddress, "The address page is invalid and breaches max page count");

        // Long here is required! Otherwise int overflow will turn it to negative value!
        // ReSharper disable once SuggestVarOrType_BuiltInTypes
        long offset = ((long)(int)address) * Page.PageSize;
        return new Page((byte*)Ptr + offset);
    }

    private DbAddress GetAddress(in Page page)
    {
        return DbAddress.Page((uint)(Unsafe
            .ByteOffset(ref Unsafe.AsRef<byte>(Ptr), ref Unsafe.AsRef<byte>(page.Raw.ToPointer()))
            .ToInt64() / Page.PageSize));
    }

    public abstract void Dispose();
    protected abstract void Flush();

    /// <summary>
    /// Begins a batch representing the next block.
    /// </summary>
    /// <returns></returns>
    public IBatch BeginNextBlock() => BuildFromRoot(Root);

    /// <summary>
    /// Reorganizes chain back to the given block hash and starts building on top of it.
    /// </summary>
    /// <param name="stateRootHash">The block hash to reorganize to.</param>
    /// <returns>The new batch.</returns>
    public IBatch ReorganizeBackToAndStartNew(Keccak stateRootHash)
    {
        RootPage? reorganizeTo = default;

        // find block with the given state root hash
        foreach (var rootPage in _roots)
        {
            if (rootPage.Data.StateRootHash == stateRootHash)
            {
                reorganizeTo = rootPage;
            }
        }

        if (reorganizeTo == null)
        {
            throw new ArgumentException(
                $"The block with the stateRootHash equal to '{stateRootHash}' was not found across history of recent {_historyDepth} blocks kept in Paprika",
                nameof(stateRootHash));
        }

        return BuildFromRoot(reorganizeTo.Value);
    }

    public IReadOnlyBatch BeginReadOnlyBatch()
    {
        lock (_batchLock)
        {
            var data = new DataPage(GetAt(Root.Data.DataPage));
            var batchId = Root.Header.BatchId;
            var batch = new ReadOnlyBatch(this, batchId, data);
            _batchesReadOnly.Add(batch);
            return batch;
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

            // move to the next block
            root.Data.BlockNumber++;

            // select min batch across the one respecting history and the min of all the read-only batches
            var minBatch = root.Header.BatchId - _historyDepth;
            foreach (var batch in _batchesReadOnly)
            {
                minBatch = Math.Min(batch.BatchId, minBatch);
            }

            return _batchCurrent = new Batch(this, root, minBatch, ctx);
        }
    }

    private void SetNewRoot(RootPage root)
    {
        _lastRoot += 1;
        root.CopyTo(_roots[_lastRoot % _historyDepth]);
    }

    class ReadOnlyBatch : IReadOnlyBatch, IReadOnlyBatchContext
    {
        private readonly PagedDb _db;
        private bool _disposed;

        private readonly DataPage _data;

        public ReadOnlyBatch(PagedDb db, uint batchId, DataPage data)
        {
            _db = db;
            _data = data;
            BatchId = batchId;
        }

        public void Dispose()
        {
            _disposed = true;
            _db.DisposeReadOnlyBatch(this);
        }

        public Account GetAccount(in Keccak key)
        {
            if (_disposed)
                throw new ObjectDisposedException("The readonly batch has already been disposed");

            _data.GetAccount(key, this, out var account, RootLevel);
            return account;
        }

        public uint BatchId { get; }
        public Page GetAt(DbAddress address) => _db.GetAt(address);
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

        /// <summary>
        /// A pool of pages that are abandoned during this batch.
        /// </summary>
        private readonly Queue<DbAddress> _abandoned;

        public Batch(PagedDb db, RootPage root, uint reusePagesOlderThanBatchId, Context ctx) : base(root.Header.BatchId)
        {
            _db = db;
            _root = root;
            _reusePagesOlderThanBatchId = reusePagesOlderThanBatchId;
            _ctx = ctx;
            _unusedPool = ctx.Unused;
            _abandoned = ctx.Abandoned;
        }

        public Account GetAccount(in Keccak key)
        {
            CheckDisposed();

            var root = _root.Data.DataPage;
            if (root.IsNull)
            {
                return default;
            }

            // treat as data page
            var data = new DataPage(_db.GetAt(root));

            data.GetAccount(key, this, out var account, RootLevel);
            return account;
        }

        public void Set(in Keccak key, in Account account)
        {
            CheckDisposed();

            ref var root = ref _root.Data.DataPage;
            var page = root.IsNull ? GetNewPage(out root, true) : GetAt(root);

            // treat as data page
            var data = new DataPage(page);

            var ctx = new SetContext(in key, account.Balance, account.Nonce);

            var updated = data.Set(ctx, this, RootLevel);

            _root.Data.DataPage = _db.GetAddress(updated);
        }

        private void CheckDisposed()
        {
            if (_disposed)
            {
                throw new ObjectDisposedException("This batch has been disposed already.");
            }
        }

        public Keccak Commit(CommitOptions options)
        {
            CheckDisposed();

            CalculateStateRootHash();

            // memoize the abandoned so that it's preserved for future uses 
            MemoizeAbandoned();

            // flush data first
            _db.Flush();

            lock (_db._batchLock)
            {
                Debug.Assert(ReferenceEquals(this, _db._batchCurrent));

                _db.SetNewRoot(_root);

                if (options == CommitOptions.FlushDataAndRoot)
                {
                    // TODO: make it flush only the root page, not the whole file
                    _db.Flush();
                }

                _db._batchCurrent = null;

                return _root.Data.StateRootHash;
            }
        }

        private void CalculateStateRootHash()
        {
            // TODO: it's a dummy implementation now as there's no Merkle construct.
            // when implementing, this will be the place to put the real Keccak
            Span<byte> span = stackalloc byte[4];
            BinaryPrimitives.WriteUInt32LittleEndian(span, _root.Data.BlockNumber);
            _root.Data.StateRootHash = Keccak.Compute(span);
        }

        public override Page GetAt(DbAddress address) => _db.GetAt(address);

        public override DbAddress GetAddress(Page page) => _db.GetAddress(page);

        public override Page GetNewPage(out DbAddress addr, bool clear)
        {
            if (TryGetNoLongerUsedPage(out addr, out var registerForGC))
            {
                // succeeded getting the page from no longer used, register for gc if needed
                if (registerForGC.IsNull == false)
                {
                    RegisterForFutureReuse(_db.GetAt(registerForGC));
                }
            }
            else
            {
                // on failure to reuse a page, default to allocating a new one.
                addr = _root.Data.GetNextFreePage();
            }

            Debug.Assert(addr.Raw < _db._maxPage, "The database breached its size! The returned page is invalid");

            var page = _db.GetAt(addr);
            if (clear)
                page.Clear();

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

            var first = new AbandonedPage(GetNewPage(out var firstAddr, true))
            {
                AbandonedAtBatch = BatchId
            };
            var last = first;

            // process abandoned
            while (_abandoned.TryDequeue(out var page))
            {
                Debug.Assert(page.IsValidPageAddress, "Only valid pages should be reused");

                last = last.EnqueueAbandoned(this, _db.GetAddress(last.AsPage()), page);
            }

            // process unused
            while (_unusedPool.TryDequeue(out var page))
            {
                last = last.EnqueueAbandoned(this, _db.GetAddress(last.AsPage()), page);
            }

            // remember the abandoned by either finding an empty slot, or chaining it to the highest number there
            var nullAddress = abandonedPages.IndexOf(DbAddress.Null);
            if (nullAddress != -1)
            {
                abandonedPages[nullAddress] = firstAddr;
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

        private bool TryGetNoLongerUsedPage(out DbAddress found, out DbAddress registerForGC)
        {
            registerForGC = default;

            // check whether previous operations allocated the pool of unused pages
            if (_unusedPool.Count == 0)
            {
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

                        // 2. register the oldest page for reuse, without using RegisterForFutureReuse that may be a re-entrant
                        registerForGC = _db.GetAddress(oldest.AsPage());

                        // 3. write its next in place of this page so that it's used as the pool
                        var indexInRoot = abandonedPages.IndexOf(oldestAddress);
                        abandonedPages[indexInRoot] = oldest.Next;
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
        public Context()
        {
            Page = new Page((byte*)NativeMemory.AlignedAlloc((UIntPtr)Page.PageSize, (UIntPtr)UIntPtr.Size));
            Abandoned = new Queue<DbAddress>();
            Unused = new Queue<DbAddress>();
        }

        public Page Page { get; }

        public Queue<DbAddress> Unused { get; }

        public Queue<DbAddress> Abandoned { get; }

        public void Clear()
        {
            Unused.Clear();
            Abandoned.Clear();

            // no need to clear, it's always overwritten
            //Page.Clear();
        }
    }
}