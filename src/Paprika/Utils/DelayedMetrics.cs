using System.Diagnostics.Metrics;

namespace Paprika.Utils;

/// <summary>
/// The delayed metrics to amortize access to the regular metrics in case the reporter is terribly slow and impact the how path.
/// </summary>
public static class DelayedMetrics
{
    private interface IAtomicIncrement<T>
    {
        T Add(ref T dest, T delta);
    }

    private struct LongIncrement : IAtomicIncrement<long>
    {
        public long Add(ref long dest, long delta) => Interlocked.Add(ref dest, delta);
    }

    private struct IntIncrement : IAtomicIncrement<int>
    {
        public int Add(ref int dest, int delta) => Interlocked.Add(ref dest, delta);
    }

    /// <summary>
    /// Returns the delayed reporting counter, that reports back to the original counter on the disposal.
    /// </summary>
    public static ICounter<int> Delay(this Counter<int> counter) => new DelayedCounter<int, IntIncrement>(counter);

    /// <summary>
    /// Returns the delayed reporting counter, that reports back to the original counter on the disposal.
    /// </summary>
    public static ICounter<long> Delay(this Counter<long> counter) => new DelayedCounter<long, LongIncrement>(counter);

    /// <summary>
    /// A <see cref="Counter{T}"/> like object that sums up all the deltas and reports back on disposal.
    /// </summary>
    public interface ICounter<in T> : IDisposable
        where T : struct
    {
        void Add(T delta);
    }

    private sealed class DelayedCounter<T, TAtomic>(Counter<T>? counter) : ICounter<T>
        where T : struct
            where TAtomic : struct, IAtomicIncrement<T>
    {
        private T _value;

        public void Add(T delta) => default(TAtomic).Add(ref _value, delta);

        public void Dispose()
        {
            counter?.Add(_value);
            counter = null;
        }
    }
}