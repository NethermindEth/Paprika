using System.Collections.Concurrent;
using System.Runtime.InteropServices;
using Paprika.Store;

namespace Paprika.Chain;

/// <summary>
/// A simple page pool, that creates slabs of pages and allows for their reuse.
/// </summary>
public class PagePool
{
    private readonly int _pagesInOneSlab;
    private readonly ConcurrentQueue<Page> _pool = new();

    // TODO: if data sets are big, we may need to go more fancy than 2 big dictionaries
    private readonly ConcurrentDictionary<DbAddress, Page> _address2Page = new();
    private readonly ConcurrentDictionary<Page, DbAddress> _page2Address = new();
    private uint _allocated;

    public PagePool(int pagesInOneSlab)
    {
        _pagesInOneSlab = pagesInOneSlab;
    }

    public Page Get()
    {
        if (_pool.TryDequeue(out var existing))
        {
            return existing;
        }

        unsafe
        {
            var allocSize = (UIntPtr)(_pagesInOneSlab * Page.PageSize);
            var allocated = (byte*)NativeMemory.AlignedAlloc(allocSize, (UIntPtr)Page.PageSize);

            // enqueue all but first
            for (var i = 1; i < _pagesInOneSlab; i++)
            {
                var page = new Page(allocated + Page.PageSize * i);
                var address = DbAddress.Page(Interlocked.Increment(ref _allocated));

                _page2Address[page] = address;
                _address2Page[address] = page;

                _pool.Enqueue(page);
            }

            return new Page(allocated);
        }
    }

    public void Return(Page page) => _pool.Enqueue(page);

    public Page GetAt(DbAddress addr) => _address2Page[addr];
    public DbAddress GetAddress(Page page) => _page2Address[page];
}