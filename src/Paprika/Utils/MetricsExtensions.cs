using System.Diagnostics.Metrics;

namespace Paprika.Utils;

public static class MetricsExtensions
{
    private class AtomicIntGauge : IAtomicIntGauge
    {
        private int _value;

        public void Add(int value) => Interlocked.Add(ref _value, value);

        public int Read() => Volatile.Read(ref _value);

        public override string ToString() => $"{Read()}";
    }

    public interface IAtomicIntGauge
    {
        public int Read();

        public void Add(int value);

        public void Subtract(int value) => Add(-value);
    }

    public static IAtomicIntGauge CreateAtomicObservableGauge(this Meter meter, string name, string? unit = null,
        string? description = null)
    {
        var atomic = new AtomicIntGauge();

        meter.CreateObservableCounter(name, atomic.Read, unit, description);

        return atomic;
    }
}