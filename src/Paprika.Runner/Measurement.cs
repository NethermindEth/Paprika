using System.Diagnostics.Metrics;
using Spectre.Console;
using Spectre.Console.Rendering;

namespace Paprika.Runner;

abstract class Measurement : JustInTimeRenderable
{
    private const long NoValue = Int64.MaxValue;
    
    private long _value;

    protected override IRenderable Build()
    {
        var value = Volatile.Read(ref _value);
        return value == NoValue ? new Text("") : new Text(value.ToString());
    }

    public void Update(double measurement, ReadOnlySpan<KeyValuePair<string,object?>> tags)
    {
        var updated = Update(measurement);
        var previous = Interlocked.Exchange(ref _value, updated);

        if (updated != previous)
        {
            MarkAsDirty();
        }
    }

    protected abstract long Update(double measurement);

    public static Measurement Build(Instrument instrument)
    {
        var type = instrument.GetType();
        if (type.IsGenericType)
        {
            var definition = type.GetGenericTypeDefinition();
            
            if (definition == typeof(ObservableGauge<>))
            {
                return new GaugeMeasurement();
            }

            if (definition == typeof(Counter<>))
            {
                return new CounterMeasurement();
            }

            if (definition == typeof(Histogram<>))
            {
                return new NoMeasurement();
            }
        }

        throw new NotImplementedException($"Not implemented for type {type}");
    }

    private class GaugeMeasurement : Measurement
    {
        protected override long Update(double measurement) => (long)measurement;
    }
    
    private class NoMeasurement : Measurement
    {
        protected override long Update(double measurement) => NoValue;
    }
    
    private class CounterMeasurement : Measurement
    {
        private long _sum;
        
        protected override long Update(double measurement) => Interlocked.Add(ref _sum, (long)measurement);
    }
} 