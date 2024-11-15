// #define STACK_TRACE_TRACKING

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

#if STACK_TRACE_TRACKING
    private readonly ConcurrentDictionary<Page, StackTrace> _traces = new();
#endif

    private readonly MetricsExtensions.IAtomicIntGauge? _allocatedMB;

    public BufferPool(int buffersInOneSlab, bool assertCountOnDispose = true, Meter? meter = null)
    {
        _buffersInOneSlab = buffersInOneSlab;
        _assertCountOnDispose = assertCountOnDispose;

        if (meter != null)
        {
            _allocatedMB =
                meter.CreateAtomicObservableGauge("BufferPool size", "MB", "The amount of MB allocated in the pool");
        }
    }

    public int? AllocatedMB => _allocatedMB?.Read();

    public unsafe Page Rent(bool clear = true)
    {
        Page pooled;
        while (_pool.TryDequeue(out pooled) == false)
        {
            var allocSize = _buffersInOneSlab * BufferSize;

            _allocatedMB?.Add(allocSize / 1024 / 1024);

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

#if STACK_TRACE_TRACKING
        _traces[pooled] = new StackTrace();
#endif

        return pooled;
    }

    public void Return(Page page)
    {
#if STACK_TRACE_TRACKING
        _traces.TryRemove(page, out _);
#endif

        _pool.Enqueue(page);
    }

    public void Dispose()
    {
        var expectedCount = _buffersInOneSlab * _slabs.Count;
        var actualCount = _pool.Count;

#if STACK_TRACE_TRACKING
        foreach (var (_, value) in _traces)
        {
            throw new Exception(value.ToString());
        }
#endif

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
    }
}