using System.Diagnostics;
using System.Diagnostics.Metrics;

namespace Paprika.Utils;

public static class MetricsExtensions
{
    public readonly struct MeasurementScope(Stopwatch sw, Histogram<long> timer) : IDisposable
    {
        public void Dispose() => timer.Record(sw.ElapsedMilliseconds);
    }

    public static MeasurementScope Measure(this Histogram<long> timer) => new(Stopwatch.StartNew(), timer);

    private class AtomicIntGauge : IAtomicIntGauge
    {
        private int _value;

        public void Set(int value) => Interlocked.Exchange(ref _value, value);

        public void Add(int value) => Interlocked.Add(ref _value, value);

        public int Read() => Volatile.Read(ref _value);

        public override string ToString() => $"{Read()}";
    }

    public interface IAtomicIntGauge
    {
        public int Read();

        public void Set(int value);

        public void Add(int value);

        public void Subtract(int value) => Add(-value);
    }

    public static IAtomicIntGauge CreateAtomicObservableGauge(this Meter meter, string name, string? unit = null,
        string? description = null)
    {
        var atomic = new AtomicIntGauge();

        meter.CreateObservableGauge(name, atomic.Read, unit, description);

        return atomic;
    }
}
