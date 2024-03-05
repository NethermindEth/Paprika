using System.Collections.Concurrent;
using System.Diagnostics.Metrics;
using System.Net.Mime;
using System.Reflection;
using System.Runtime.InteropServices;
using HdrHistogram;
using Paprika.Merkle;
using Paprika.Store;

namespace Paprika.Utils;

public interface IMeasurementValue
{
    long Value { get; }
}

public sealed class Metrics : IDisposable
{
    private readonly object _sync = new();

    private readonly MeterListener _listener;
    private readonly Dictionary<Meter, Dictionary<Instrument, IMeasurement>> _instrument2State = new();
    private readonly Dictionary<string, object> _measures;

    public MerkleStats Merkle { get; }

    public DbStats Db { get; }

    public Metrics()
    {
        Merkle = new MerkleStats();
        Db = new DbStats();

        _measures = new()
        {
            { ComputeMerkleBehavior.MeterName, Merkle },
            { PagedDb.MeterName, Db }
        };

        _listener = new MeterListener
        {
            InstrumentPublished = (instrument, listener) =>
            {
                if (HasMeterListener(instrument) == false)
                    return;

                var meter = instrument.Meter;
                var stats = _measures[meter.Name];
                var measure = stats.GetType().GetProperties().SingleOrDefault(p =>
                    p.GetCustomAttribute<InstrumentNameAttribute>()?.Instrument == instrument.Name);

                if (measure == null)
                    return;

                lock (_sync)
                {
                    ref var dict = ref CollectionsMarshal.GetValueRefOrAddDefault(_instrument2State, meter,
                        out var exists);

                    if (!exists)
                    {
                        dict = new Dictionary<Instrument, IMeasurement>();
                    }

                    var state = Measurement.Build(instrument);
                    dict!.Add(instrument, state);
                    measure.SetMethod!.Invoke(stats,
                        BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic, null,
                        new object[] { state }, null);

                    listener.EnableMeasurementEvents(instrument, state);
                }
            },
            MeasurementsCompleted = (instrument, cookie) =>
            {
                if (HasMeterListener(instrument) == false)
                    return;

                lock (_sync)
                {
                    var instruments = _instrument2State[instrument.Meter];
                    instruments.Remove(instrument, out _);
                    if (instruments.Count == 0)
                        _instrument2State.Remove(instrument.Meter);
                }
            }
        };

        _listener.Start();

        _listener.SetMeasurementEventCallback<double>((i, m, l, c) => ((IMeasurement)c!).Update(m, l));
        _listener.SetMeasurementEventCallback<float>((i, m, l, c) => ((IMeasurement)c!).Update(m, l));
        _listener.SetMeasurementEventCallback<long>((i, m, l, c) => ((IMeasurement)c!).Update(m, l));
        _listener.SetMeasurementEventCallback<int>((i, m, l, c) => ((IMeasurement)c!).Update(m, l));
        _listener.SetMeasurementEventCallback<short>((i, m, l, c) => ((IMeasurement)c!).Update(m, l));
        _listener.SetMeasurementEventCallback<byte>((i, m, l, c) => ((IMeasurement)c!).Update(m, l));
        _listener.SetMeasurementEventCallback<decimal>((i, m, l, c) => ((IMeasurement)c!).Update((double)m, l));
    }

    private bool HasMeterListener(Instrument instrument) => _measures.ContainsKey(instrument.Meter.Name);

    public void Observe() => _listener.RecordObservableInstruments();

    public void Dispose() => _listener.Dispose();

    interface IMeasurement
    {
        void Update(double measurement, ReadOnlySpan<KeyValuePair<string, object?>> tags);
    }

    abstract class Measurement : IMeasurement, IMeasurementValue
    {
        private readonly Instrument _instrument;

        private long _value;

        public void Update(double measurement, ReadOnlySpan<KeyValuePair<string, object?>> tags) =>
            Volatile.Write(ref _value, Update(measurement));

        protected abstract long Update(double measurement);

        private Measurement(Instrument instrument)
        {
            _instrument = instrument;
        }

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
                    return new HistogramHdrMeasurement(instrument);
                }
            }

            throw new NotImplementedException($"Not implemented for type {type}");
        }

        private class GaugeMeasurement(Instrument instrument) : Measurement(instrument)
        {
            protected override long Update(double measurement) => (long)measurement;
        }

        // for now use the last value
        private class HistogramLastMeasurement(Instrument instrument) : Measurement(instrument)
        {
            protected override long Update(double measurement) => (long)measurement;
        }

        private class HistogramHdrMeasurement : Measurement
        {
            private const double Percentile = 99;
            private readonly LongConcurrentHistogram _histogram;

            public HistogramHdrMeasurement(Instrument instrument) : base(instrument)
            {
                _histogram = new LongConcurrentHistogram(1, 1, int.MaxValue, 4);
            }

            protected override long Update(double measurement)
            {
                _histogram.RecordValue((long)measurement);
                try
                {
                    return _histogram.GetValueAtPercentile(Percentile);
                }
                catch (Exception)
                {
                    return default;
                }
            }
        }

        private class CounterMeasurement(Instrument instrument) : Measurement(instrument)
        {
            private long _sum;

            protected override long Update(double measurement) => Interlocked.Add(ref _sum, (long)measurement);
        }

        public long Value => Volatile.Read(ref _value);
    }
}

public class MerkleStats
{
    [InstrumentName(ComputeMerkleBehavior.TotalMerkle)]
    public IMeasurementValue TotalMerkle { get; private set; }

    [InstrumentName(ComputeMerkleBehavior.HistogramStateProcessing)]
    public IMeasurementValue StateProcessing { get; private set; }

    [InstrumentName(ComputeMerkleBehavior.HistogramStorageProcessing)]
    public IMeasurementValue StorageProcessing { get; private set; }
}

public class DbStats
{
    [InstrumentName(PagedDb.DbSize)]
    public IMeasurementValue DbSize { get; private set; }
}

[AttributeUsage(AttributeTargets.Property)]
file class InstrumentNameAttribute(string instrument) : Attribute
{
    public string Instrument { get; } = instrument;
}
