using System.Collections.Concurrent;
using System.Diagnostics;
using System.Diagnostics.Metrics;
using System.Runtime.InteropServices;
using Paprika.Store;
using Paprika.Utils;

namespace Paprika.Chain;

/// <summary>
/// The default page sized pool
/// </summary>
public sealed class BufferPool : BufferPool<PageSize>
{
    public static int BufferSize => PageSize.BufferSize;

    public BufferPool(int buffersInOneSlab, bool assertCountOnDispose = true, Meter? meter = null)
        : base(buffersInOneSlab, assertCountOnDispose, meter)
    {
    }
}

public struct PageSize : ISize
{
    public static int BufferSize => Page.PageSize;
}

public interface ISize
{
    static abstract int BufferSize { get; }
}

/// <summary>
/// A simple page pool, that creates slabs of pages and allows for their reuse.
/// </summary>
public class BufferPool<TSize> : IDisposable
    where TSize : struct, ISize
{
    private readonly int _buffersInOneSlab;
    private readonly bool _assertCountOnDispose;
    private readonly ConcurrentQueue<Page> _pool = new();
    private readonly ConcurrentQueue<IntPtr> _slabs = new();

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
        var size = TSize.BufferSize;

        Page pooled;
        while (_pool.TryDequeue(out pooled) == false)
        {
            var allocSize = _buffersInOneSlab * size;

            _allocatedMB?.Add(allocSize / 1024 / 1024);

            var slab = (byte*)NativeMemory.AlignedAlloc((UIntPtr)allocSize, (UIntPtr)size);

            _slabs.Enqueue(new IntPtr(slab));

            // enqueue all
            for (var i = 0; i < _buffersInOneSlab; i++)
            {
                var page = new Page(slab + size * i);
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
    }
}