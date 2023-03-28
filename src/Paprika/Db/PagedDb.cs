using System.Collections.Concurrent;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
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
        for (var i = 0; i < MinHistoryDepth; i++)
        {
            _roots[i] = new RootPage(GetAt(i));
        }

        if (_roots[0].NextFreePage < _historyDepth)
        {
            // the 0th page will have the properly number set to first free page
            _roots[0].NextFreePage = _historyDepth;
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

    public double TotalUsedPages => (double)Root.NextFreePage / _maxPage;

    private RootPage Root => _roots[_lastRoot % _historyDepth];

    private void MoveRootNext() => _lastRoot++;

    private Page GetAt(int address)
    {
        if (address > _maxPage)
            throw new ArgumentException($"Requested address {address} while the max page is {_maxPage}");

        // Long here is required! Otherwise int overflow will turn it to negative value!
        // ReSharper disable once SuggestVarOrType_BuiltInTypes
        long offset = ((long)address) * Page.PageSize;
        return new Page((byte*)Ptr + offset);
    }

    private int GetAddress(in Page page)
    {
        return (int)(Unsafe.ByteOffset(ref Unsafe.AsRef<byte>(Ptr), ref Unsafe.AsRef<byte>(page.Raw.ToPointer()))
            .ToInt64() / Page.PageSize);
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
    public ITransaction Begin() => new Transaction(this, RentRootPage());

    private void SetNewRoot(RootPage root)
    {
        _lastRoot += 1;
        root.CopyTo(_roots[_lastRoot % _historyDepth]);
        ReleaseRootPage(root);
    }

    class Transaction : ITransaction, IInternalTransaction
    {
        private readonly PagedDb _db;
        private readonly RootPage _root;
        private readonly long _txId;

        public Transaction(PagedDb db, RootPage tempRootPage)
        {
            _db = db;
            _root = tempRootPage;

            _db.Root.CopyTo(_root);

            _txId = _root.Header.TransactionId++;
        }

        // public bool TryGet(in ReadOnlySpan<byte> key, out ReadOnlySpan<byte> value)
        // {
        //     var path = NibblePath.FromKey(key);
        //     return _root.TryGet(path, out value, 0, this);
        // }
        //
        // public void Set(in ReadOnlySpan<byte> key, in ReadOnlySpan<byte> value)
        // {
        //     var path = NibblePath.FromKey(key);
        //     _root = _root.Set(path, value, 0, this);
        // }

        public void Commit(CommitOptions options)
        {
            // flush data first
            _db.Flush();
            _db.SetNewRoot(_root);

            if (options == CommitOptions.FlushDataAndRoot)
            {
                _db.Flush();
            }
        }

        public double TotalUsedPages => (double)_root.NextFreePage / _db._maxPage;

        Page IInternalTransaction.GetAt(int address) => _db.GetAt(address);

        int IInternalTransaction.GetAddress(in Page page) => _db.GetAddress(page);

        Page IInternalTransaction.GetNewDirtyPage(out int addr)
        {
            addr = _root.NextFreePage++;

            if (addr >= _db._maxPage)
                throw new Exception("The db file is too small for this page");

            return _db.GetAt(addr);
        }
    }
}