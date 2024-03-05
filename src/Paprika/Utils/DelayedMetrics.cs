using System.Diagnostics.Metrics;

namespace Paprika.Utils;

/// <summary>
/// The delayed metrics to amortize access to the regular metrics in case the reporter is terribly slow and impact the how path.
/// </summary>
public static class DelayedMetrics
{
    public interface IAtomicIncrement<T>
    {
        T Add(ref T dest, T delta);
    }

    public struct LongIncrement : IAtomicIncrement<long>
    {
        public long Add(ref long dest, long delta) => Interlocked.Add(ref dest, delta);
    }

    public struct IntIncrement : IAtomicIncrement<int>
    {
        public int Add(ref int dest, int delta) => Interlocked.Add(ref dest, delta);
    }

    /// <summary>
    /// Returns the delayed reporting counter, that reports back to the original counter on the disposal.
    /// </summary>
    public static DelayedCounter<int, IntIncrement> Delay(this Counter<int> counter) => new(counter);

    /// <summary>
    /// Returns the delayed reporting counter, that reports back to the original counter on the disposal.
    /// </summary>
    public static DelayedCounter<long, LongIncrement> Delay(this Counter<long> counter) => new(counter);

    public sealed class DelayedCounter<T, TAtomic>(Counter<T>? counter)
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