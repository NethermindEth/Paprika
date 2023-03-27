﻿using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

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
    /// If history depth is set to the max reorg depth,
    /// moving to previous block is just a single write transaction moving the root back.
    ///
    /// ABANDONED PAGES
    /// To keep N roots active, the pages that were abandoned in previous transactions should be reused only on
    /// rolling over, meaning, that they should be taken from the item that the root will point to. In this case,
    /// if undo happens, they are still active. So use abandoned pages and add them to abandoned pages of the given
    /// transaction, use at will and commit them so that they can be reused in <see cref="HistoryDepth"/> commits. 
    /// </remarks>
    private const int HistoryDepth = 2;

    private readonly int _maxPage;
    private readonly MetadataPage*[] _metadata;

    private long _currentRoot;

    [StructLayout(LayoutKind.Explicit, Size = Page.PageSize, Pack = 1)]
    private struct MetadataPage
    {
        [FieldOffset(0)] public int NextFreePage;
        [FieldOffset(4)] public int Root;
        [FieldOffset(8)] public long TxId;

        /// <summary>
        /// Pops the next free page.
        /// </summary>
        public int PopNextFreePage()
        {
            // TODO: reuse empty pages
            return NextFreePage++;
        }

        public void Abandon(int address)
        {
            // TODO: mark page as abandoned
        }

        public void PrepareCommit()
        {
            // TODO: clear pages bits, calculate keccaks etc.
        }
    }

    protected PagedDb(ulong size)
    {
        _maxPage = (int)(size / Page.PageSize);
        _metadata = new MetadataPage*[HistoryDepth];
    }

    protected void RootInit()
    {
        for (var i = 0; i < HistoryDepth; i++)
        {
            _metadata[i] = GetAt(i).As<MetadataPage>();
        }

        if (_metadata[0]->NextFreePage < HistoryDepth)
        {
            // the 0th page will have the properly number set to first free page
            _metadata[0]->NextFreePage = HistoryDepth;
        }

        _currentRoot = 0;
        for (var i = 0; i < HistoryDepth; i++)
        {
            if (_metadata[i]->TxId > _currentRoot)
            {
                _currentRoot = _metadata[i]->TxId;
            }
        }
    }

    protected abstract void* Ptr { get; }

    public double TotalUsedPages => (double)CurrentMeta->NextFreePage / _maxPage;

    private MetadataPage* CurrentMeta => _metadata[_currentRoot % HistoryDepth];
    private MetadataPage* NextMeta => _metadata[(_currentRoot + 1) % HistoryDepth];

    private void MoveRootNext() => _currentRoot++;

    private Page GetAt(int address)
    {
        if (address > _maxPage)
            throw new ArgumentException($"Requested address {address} while the max page is {_maxPage}");

        // Long here is required! Otherwise int overflow will turn it to negative value!
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

    public ITransaction Begin() => new Transaction(this);

    class Transaction : ITransaction, IInternalTransaction
    {
        private readonly PagedDb _db;
        private readonly MetadataPage* _meta;
        private Page _root;

        public Transaction(PagedDb db)
        {
            _db = db;

            // copy to next meta
            *_db.NextMeta = *_db.CurrentMeta;
            _meta = _db.NextMeta;

            // set next id
            _meta->TxId++;

            // peek the next free and treat it as root
            var newRoot = _meta->PopNextFreePage();
            _root = _db.GetAt(newRoot);
            _meta->Root = newRoot;
            _root.Clear();

            // copy current
            if (_db.CurrentMeta->Root != 0)
            {
                _db.GetAt(_db.CurrentMeta->Root).CopyTo(_root);

                // abandon current
                _db.NextMeta->Abandon(_db.CurrentMeta->Root);
            }
        }

        public bool TryGet(in ReadOnlySpan<byte> key, out ReadOnlySpan<byte> value)
        {
            var path = NibblePath.FromKey(key);
            return _root.TryGet(path, out value, 0, this);
        }

        public void Set(in ReadOnlySpan<byte> key, in ReadOnlySpan<byte> value)
        {
            var path = NibblePath.FromKey(key);
            _root = _root.Set(path, value, 0, this);
        }

        public void Commit()
        {
            // flush data first
            _meta->Root = _db.GetAddress(_root);
            _meta->PrepareCommit();
            _root.ClearWritable();
            _db.Flush();

            // set and flush next root
            _db.MoveRootNext();
            _db.Flush();
        }

        public double TotalUsedPages => (double)_meta->NextFreePage / _db._maxPage;

        Page IInternalTransaction.GetAt(int address) => _db.GetAt(address);

        int IInternalTransaction.GetAddress(in Page page) => _db.GetAddress(page);

        Page IInternalTransaction.GetNewDirtyPage(out int addr)
        {
            addr = _meta->PopNextFreePage();

            if (addr >= _db._maxPage)
                throw new Exception("The db file is too small for this page");

            return _db.GetAt(addr);
        }

        void IInternalTransaction.Abandon(in Page page)
        {
            _meta->Abandon(_db.GetAddress(page));
        }
    }
}