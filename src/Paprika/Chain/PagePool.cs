using System.Collections.Concurrent;
using System.Diagnostics;
using System.Diagnostics.Metrics;
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
    private readonly ConcurrentQueue<IntPtr> _slabs = new();

    private int _allocatedPages;

    // metrics
    private readonly Meter _meter;

    public PagePool(int pagesInOneSlab)
    {
        _pagesInOneSlab = pagesInOneSlab;

        _meter = new Meter("Paprika.Chain.PagePool");
        _meter.CreateObservableCounter("Allocated Pages", () => Volatile.Read(ref _allocatedPages), "4kb page",
            "the number of pages allocated in the page pool");
    }

    public int AllocatedPages => _allocatedPages;

    public unsafe Page Rent(bool clear = true)
    {
        Page pooled;
        while (_pool.TryDequeue(out pooled) == false)
        {
            Interlocked.Add(ref _allocatedPages, _pagesInOneSlab);

            var allocSize = (UIntPtr)(_pagesInOneSlab * Page.PageSize);
            var slab = (byte*)NativeMemory.AlignedAlloc(allocSize, (UIntPtr)Page.PageSize);

            _slabs.Enqueue(new IntPtr(slab));

            // enqueue all
            for (var i = 0; i < _pagesInOneSlab; i++)
            {
                var page = new Page(slab + Page.PageSize * i);
                _pool.Enqueue(page);
            }
        }

        if (clear)
            pooled.Clear();

        return pooled;
    }

    public void Return(Page page) => _pool.Enqueue(page);

    public void Dispose()
    {
        var expectedCount = _pagesInOneSlab * _slabs.Count;
        var actualCount = _pool.Count;
        Debug.Assert(expectedCount == actualCount,
            $"There should be {expectedCount} pages in the pool but there are only {actualCount}");

        while (_slabs.TryDequeue(out var slab))
        {
            unsafe
            {
                NativeMemory.AlignedFree(slab.ToPointer());
            }
        }

        _meter.Dispose();
    }
}