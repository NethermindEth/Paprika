using System.Buffers.Binary;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Nethermind.Int256;
using Paprika.Crypto;
using Paprika.Pages;

namespace Paprika.Db;

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
    private readonly ConcurrentStack<RootPage> _pool = new();

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
            if (_roots[i].Header.TransactionId > _lastRoot)
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
        Debug.Assert(address.IsValidAddressPage, "The address page is invalid and breaches max page count");

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

    private RootPage RentRootPage()
    {
        if (_pool.TryPop(out RootPage page))
        {
            return page;
        }

        var memory = (byte*)NativeMemory.AlignedAlloc((UIntPtr)Page.PageSize, (UIntPtr)UIntPtr.Size);
        return new RootPage(memory);
    }

    private void ReleaseRootPage(RootPage page) => _pool.Push(page);

    // for now, omit block consideration
    public IBatch BeginNextBlock() => new Batch(this, RentRootPage());

    private void SetNewRoot(RootPage root)
    {
        _lastRoot += 1;
        root.CopyTo(_roots[_lastRoot % _historyDepth]);
    }

    class Batch : IBatch, IBatchContext
    {
        private const byte RootLevel = 0;
        private readonly PagedDb _db;
        private readonly RootPage _root;
        private readonly long _txId;

        public Batch(PagedDb db, RootPage tempRootPage)
        {
            _db = db;
            _root = tempRootPage;

            _db.Root.CopyTo(_root);

            _root.Header.TransactionId++;

            // for now, this handles only the forward by one block
            // when reorgs are implemented fully, this should be a jump to an arbitrary block from the kept history
            _root.Data.BlockNumber++;
            _txId = _root.Header.TransactionId;
        }

        public bool TryGetNonce(in Keccak account, out UInt256 nonce)
        {
            var root = _root.Data.DataPage;
            if (root.IsNull)
            {
                nonce = default;
                return false;
            }

            // treat as data page
            var data = new DataPage(_db.GetAt(root));

            return data.TryGetNonce(account, out nonce, RootLevel);
        }

        public void Set(in Keccak account, in UInt256 balance, in UInt256 nonce)
        {
            ref var root = ref _root.Data.DataPage;
            var page = root.IsNull ? GetNewDirtyPage(out root) : GetWritable(ref root);

            // treat as data page
            var data = new DataPage(page);

            var ctx = new SetContext(in account, balance, nonce);
            data.Set(ctx, this, RootLevel);
        }

        public Keccak Commit(CommitOptions options)
        {
            CalculateRootHash();

            // flush data first
            _db.Flush();
            _db.SetNewRoot(_root);

            if (options == CommitOptions.FlushDataAndRoot)
            {
                _db.Flush();
            }

            return _root.Data.BlockHash;
        }

        private void CalculateRootHash()
        {
            // TODO: it's a dummy implementation now as there's no Merkle construct.
            // when implementing, this will be the place to put the real Keccak
            Span<byte> span = stackalloc byte[4];
            BinaryPrimitives.WriteUInt32LittleEndian(span, _root.Data.BlockNumber);
            _root.Data.BlockHash = Keccak.Compute(span);
        }

        public double TotalUsedPages => ((double)(uint)_root.Data.NextFreePage) / _db._maxPage;

        Page IBatchContext.GetAt(DbAddress address) => _db.GetAt(address);

        DbAddress IBatchContext.GetAddress(in Page page) => _db.GetAddress(page);

        public Page GetNewDirtyPage(out DbAddress addr)
        {
            addr = _root.Data.GetNextFreePage();
            Debug.Assert(addr.IsValidAddressPage, "The page address retrieved is invalid");
            var page = _db.GetAt(addr);
            AssignTxId(page);
            return page;
        }

        private void AssignTxId(Page page) => page.Header.TransactionId = _txId;

        private Page GetWritable(ref DbAddress addr)
        {
            var page = _db.GetAt(addr);
            if (page.Header.TransactionId == _txId)
                return page;

            var @new = GetNewDirtyPage(out addr);
            page.CopyTo(@new);
            AssignTxId(@new);

            // TODO: the previous page is dangling and the only information it has is the tx_id, mem management is needed.
            // Or a process that would scan pages for being old enough to be reused

            return @new;
        }

        public void Dispose() => _db.ReleaseRootPage(_root);
    }
}