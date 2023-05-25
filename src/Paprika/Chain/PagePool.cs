using System.Collections.Concurrent;
using System.Runtime.InteropServices;
using Paprika.Store;

namespace Paprika.Chain;

/// <summary>
/// A simple page pool, that creates slabs of pages and allows for their reuse.
/// </summary>
public class PagePool : IDisposable
{
    private readonly int _pagesInOneSlab;
    private readonly ConcurrentQueue<Page> _pool = new();

    // TODO: if data sets are big, we may need to go more fancy than 2 big dictionaries
    private readonly ConcurrentDictionary<DbAddress, Page> _address2Page = new();
    private readonly ConcurrentDictionary<Page, DbAddress> _page2Address = new();
    private readonly ConcurrentQueue<IntPtr> _slabs = new();
    private uint _allocated;

    public PagePool(int pagesInOneSlab)
    {
        _pagesInOneSlab = pagesInOneSlab;
    }

    public unsafe Page Get()
    {
        Page pooled;
        while (_pool.TryDequeue(out pooled) == false)
        {
            var allocSize = (UIntPtr)(_pagesInOneSlab * Page.PageSize);
            var slab = (byte*)NativeMemory.AlignedAlloc(allocSize, (UIntPtr)Page.PageSize);

            _slabs.Enqueue(new IntPtr(slab));

            // enqueue all but first
            for (var i = 0; i < _pagesInOneSlab; i++)
            {
                var page = new Page(slab + Page.PageSize * i);
                var address = DbAddress.Page(Interlocked.Increment(ref _allocated));

                _page2Address[page] = address;
                _address2Page[address] = page;

                _pool.Enqueue(page);
            }
        }

        return pooled;
    }

    public void Return(Page page) => _pool.Enqueue(page);

    public Page GetAt(DbAddress addr) => _address2Page[addr];
    public DbAddress GetAddress(Page page) => _page2Address[page];

    public void Dispose()
    {
        while (_slabs.TryDequeue(out var slab))
        {
            unsafe
            {
                NativeMemory.AlignedFree(slab.ToPointer());
            }
        }
    }
}