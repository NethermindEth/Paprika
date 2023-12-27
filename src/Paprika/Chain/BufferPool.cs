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
    public const int BufferSize = Page.PageSize;

    private readonly int _buffersInOneSlab;
    private readonly bool _assertCountOnDispose;
    private readonly ConcurrentQueue<Page> _pool = new();
    private readonly ConcurrentQueue<IntPtr> _slabs = new();

    private readonly MetricsExtensions.IAtomicIntGauge _allocatedMB;

    // metrics
    private readonly Meter _meter;

    public BufferPool(int buffersInOneSlab, bool assertCountOnDispose = true, string name = "")
    {
        _buffersInOneSlab = buffersInOneSlab;
        _assertCountOnDispose = assertCountOnDispose;

        var baseName = "Paprika.Chain.BufferPool";

        _meter = new Meter(string.IsNullOrEmpty(name) ? baseName : baseName + "-" + name);
        _allocatedMB = _meter.CreateAtomicObservableGauge("Total buffers' size", "MB",
            "The amount of MB allocated in the pool");
    }

    public int AllocatedMB => _allocatedMB.Read();

    public unsafe Page Rent(bool clear = true)
    {
        Page pooled;
        while (_pool.TryDequeue(out pooled) == false)
        {
            var allocSize = _buffersInOneSlab * BufferSize;

            _allocatedMB.Add(allocSize / 1024 / 1024);

            var slab = (byte*)NativeMemory.AlignedAlloc((UIntPtr)allocSize, BufferSize);

            _slabs.Enqueue(new IntPtr(slab));

            // enqueue all
            for (var i = 0; i < _buffersInOneSlab; i++)
            {
                var page = new Page(slab + BufferSize * i);
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
        var expectedCount = _buffersInOneSlab * _slabs.Count;
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