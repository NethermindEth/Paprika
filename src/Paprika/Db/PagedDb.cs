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

    private readonly byte _historyDepth;
    private readonly int _maxPage;
    private long _lastRoot;
    private readonly RootPage[] _roots;

    // a simple pool of root pages
    private readonly ConcurrentStack<Page> _pool = new();

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

    private Page RentPage()
    {
        if (_pool.TryPop(out var page))
        {
            return page;
        }

        var memory = (byte*)NativeMemory.AlignedAlloc((UIntPtr)Page.PageSize, (UIntPtr)UIntPtr.Size);
        return new Page(memory);
    }

    private void ReturnPage(Page page) => _pool.Push(page);

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

    private IBatch BuildFromRoot(RootPage rootPage)
    {
        // prepare root
        var root = new RootPage(RentPage());
        rootPage.CopyTo(root);
        
        // always inc the batchId
        root.Header.BatchId++;

        // move to the next block
        root.Data.BlockNumber++;

        return new Batch(this, root);
    }

    private void SetNewRoot(RootPage root)
    {
        _lastRoot += 1;
        root.CopyTo(_roots[_lastRoot % _historyDepth]);
    }

    class Batch : BatchContextBase, IBatch
    {
        private const byte RootLevel = 0;
        private readonly PagedDb _db;
        private readonly RootPage _root;

        // TODO: when read transactions enabled, provide second parameter as
        // Math.Min(all reader transactions batches, root.Header.BatchId - db._historyDepth)
        public Batch(PagedDb db, RootPage root) : base(root.Header.BatchId, root.Header.BatchId - db._historyDepth)
        {
            _db = db;
            _root = root;
        }

        public Account GetAccount(in Keccak key)
        {
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
            ref var root = ref _root.Data.DataPage;
            var page = root.IsNull ? GetNewPage(out root, true) : GetAt(root);

            // treat as data page
            var data = new DataPage(page);

            var ctx = new SetContext(in key, account.Balance, account.Nonce);

            var updated = data.Set(ctx, this, RootLevel);

            _root.Data.DataPage = _db.GetAddress(updated);
        }

        public Keccak Commit(CommitOptions options)
        {
            CalculateStateRootHash();

            // flush data first
            _db.Flush();
            
            _db.SetNewRoot(_root);

            if (options == CommitOptions.FlushDataAndRoot)
            {
                _db.Flush();
            }

            return _root.Data.StateRootHash;
        }

        private void CalculateStateRootHash()
        {
            // TODO: it's a dummy implementation now as there's no Merkle construct.
            // when implementing, this will be the place to put the real Keccak
            Span<byte> span = stackalloc byte[4];
            BinaryPrimitives.WriteUInt32LittleEndian(span, _root.Data.BlockNumber);
            _root.Data.StateRootHash = Keccak.Compute(span);
        }

        public double TotalUsedPages => ((double)(uint)_root.Data.NextFreePage) / _db._maxPage;

        public override Page GetAt(DbAddress address) => _db.GetAt(address);


        public override Page GetNewPage(out DbAddress addr, bool clear)
        {
            addr = _root.Data.GetNextFreePage();
            Debug.Assert(addr.IsValidPageAddress, "The page address retrieved is invalid");
            var page = _db.GetAt(addr);
            if (clear)
                page.Clear();

            AssignBatchId(page);
            return page;
        }

        protected override void RegisterForFutureGC(Page page)
        {
            // _abandoned.EnqueueAbandoned(this, _db.GetAddress(page));
        }

        public void Dispose()
        {
            _db.ReturnPage(_root.AsPage());
        }
    }
}