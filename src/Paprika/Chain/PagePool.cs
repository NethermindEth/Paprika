using System.Collections.Concurrent;
using System.Runtime.InteropServices;
using Paprika.Store;

namespace Paprika.Chain;

/// <summary>
/// A simple page pool, that creates slabs of pages and allows for their reuse.
/// </summary>
public class PagePool : IDisposable
{
    private readonly uint _pagesInOneSlab;
    private readonly ConcurrentQueue<Page> _pool = new();

    private readonly ConcurrentQueue<IntPtr> _slabs = new();
    private uint _allocatedPages;

    public PagePool(uint pagesInOneSlab)
    {
        _pagesInOneSlab = pagesInOneSlab;
    }

    public uint AllocatedPages => _allocatedPages;

    public unsafe Page Rent()
    {
        Page pooled;
        while (_pool.TryDequeue(out pooled) == false)
        {
            Interlocked.Add(ref _allocatedPages, _pagesInOneSlab);

            var allocSize = (UIntPtr)(_pagesInOneSlab * Page.PageSize);
            var slab = (byte*)NativeMemory.AlignedAlloc(allocSize, (UIntPtr)Page.PageSize);

            _slabs.Enqueue(new IntPtr(slab));

            // enqueue all but first
            for (var i = 0; i < _pagesInOneSlab; i++)
            {
                var page = new Page(slab + Page.PageSize * i);
                _pool.Enqueue(page);
            }
        }

        pooled.Clear();

        return pooled;
    }

    public void Return(Page page) => _pool.Enqueue(page);

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