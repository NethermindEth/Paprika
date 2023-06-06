using System.Collections.Concurrent;
using System.Diagnostics;
using System.Diagnostics.Metrics;
using System.Runtime.InteropServices;
using Paprika.Store;
using Paprika.Utils;

namespace Paprika.Chain;

/// <summary>
/// A simple page pool, that creates slabs of pages and allows for their reuse.
/// </summary>
public class PagePool : IDisposable
{
    private readonly int _pagesInOneSlab;
    private readonly bool _assertCountOnDispose;
    private readonly ConcurrentQueue<Page> _pool = new();
    private readonly ConcurrentQueue<IntPtr> _slabs = new();

    private readonly MetricsExtensions.IAtomicIntGauge _allocatedPages;

    // metrics
    private readonly Meter _meter;

    public PagePool(int pagesInOneSlab, bool assertCountOnDispose = true)
    {
        _pagesInOneSlab = pagesInOneSlab;
        _assertCountOnDispose = assertCountOnDispose;

        _meter = new Meter("Paprika.Chain.PagePool");
        _allocatedPages = _meter.CreateAtomicObservableGauge("Allocated Pages", "4kb page",
            "the number of pages allocated in the page pool");
    }

    public int AllocatedPages => _allocatedPages.Read();

    public unsafe Page Rent(bool clear = true)
    {
        Page pooled;
        while (_pool.TryDequeue(out pooled) == false)
        {
            _allocatedPages.Add(_pagesInOneSlab);

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

        if (_assertCountOnDispose)
        {
            Debug.Assert(expectedCount == actualCount,
                $"There should be {expectedCount} pages in the pool but there are only {actualCount}");
        }

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