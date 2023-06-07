using System.Diagnostics.Metrics;
using Spectre.Console;
using Spectre.Console.Rendering;

namespace Paprika.Runner;

abstract class Measurement : JustInTimeRenderable
{
    private readonly Instrument _instrument;
    private const long NoValue = Int64.MaxValue;

    private long _value;

    private Measurement(Instrument instrument)
    {
        _instrument = instrument;
    }

    protected override IRenderable Build()
    {
        var value = Volatile.Read(ref _value);
        return value == NoValue ? new Text("") : new Text(value.ToString());
    }

    public void Update(double measurement, ReadOnlySpan<KeyValuePair<string, object?>> tags)
    {
        var updated = Update(measurement);
        var previous = Interlocked.Exchange(ref _value, updated);

        if (updated != previous)
        {
            MarkAsDirty();
        }
    }

    protected abstract long Update(double measurement);

    public override string ToString() => $"{nameof(Instrument)}: {_instrument.Name}, Value: {Volatile.Read(ref _value)}";

    public static Measurement Build(Instrument instrument)
    {
        var type = instrument.GetType();
        if (type.IsGenericType)
        {
            var definition = type.GetGenericTypeDefinition();

            if (definition == typeof(ObservableGauge<>))
            {
                return new GaugeMeasurement(instrument);
            }

            if (definition == typeof(Counter<>))
            {
                return new CounterMeasurement(instrument);
            }

            if (definition == typeof(Histogram<>))
            {
                return new HistogramMeasurement(instrument);
            }
        }

        throw new NotImplementedException($"Not implemented for type {type}");
    }

    private class GaugeMeasurement : Measurement
    {
        public GaugeMeasurement(Instrument instrument) : base(instrument)
        {
        }

        protected override long Update(double measurement) => (long)measurement;
    }

    // for now use the last value
    private class HistogramMeasurement : Measurement
    {
        protected override long Update(double measurement)
        {
            return (long)measurement;
        }

        public HistogramMeasurement(Instrument instrument) : base(instrument)
        {
        }
    }

    private class CounterMeasurement : Measurement
    {
        private long _sum;

        protected override long Update(double measurement) => Interlocked.Add(ref _sum, (long)measurement);

        public CounterMeasurement(Instrument instrument) : base(instrument)
        {
        }
    }
}