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
public class BufferPool : IDisposable
{
    private readonly int _pagesInOneSlab;
    private readonly bool _assertCountOnDispose;
    private readonly ConcurrentQueue<Page> _pool = new();
    private readonly ConcurrentQueue<IntPtr> _slabs = new();

    private readonly MetricsExtensions.IAtomicIntGauge _allocatedMB;

    // metrics
    private readonly Meter _meter;

    public BufferPool(int pagesInOneSlab, bool assertCountOnDispose = true)
    {
        _pagesInOneSlab = pagesInOneSlab;
        _assertCountOnDispose = assertCountOnDispose;

        _meter = new Meter("Paprika.Chain.BufferPool");
        _allocatedMB = _meter.CreateAtomicObservableGauge("Total buffers' size", "MB",
            "The amount of MB allocated in the pool");
    }

    public int AllocatedMB => _allocatedMB.Read();

    public unsafe Page Rent(bool clear = true)
    {
        Page pooled;
        while (_pool.TryDequeue(out pooled) == false)
        {
            var allocSize = _pagesInOneSlab * Page.PageSize;

            _allocatedMB.Add(allocSize / 1024 / 1024);

            var slab = (byte*)NativeMemory.AlignedAlloc((UIntPtr)allocSize, Page.PageSize);

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