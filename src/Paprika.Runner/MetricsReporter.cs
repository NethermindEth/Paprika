using System.Collections.Concurrent;
using System.Diagnostics.Metrics;
using System.Runtime.InteropServices;
using Spectre.Console;
using Spectre.Console.Rendering;

namespace Paprika.Runner;

public class MetricsReporter : IDisposable
{
    private readonly object _sync = new();
    private readonly MeterListener _listener;
    private readonly Dictionary<Meter, Dictionary<Instrument, Measurement>> _instrument2State = new();
    private readonly Table _table;

    public IRenderable Renderer => _table;

    public MetricsReporter()
    {
        _table = new Table();

        _table.AddColumn(new TableColumn("Meter").LeftAligned());
        _table.AddColumn(new TableColumn("Instrument").LeftAligned());
        _table.AddColumn(new TableColumn("Value").Width(10).RightAligned());
        _table.AddColumn(new TableColumn("Unit").RightAligned());

        _listener = new MeterListener
        {
            InstrumentPublished = (instrument, listener) =>
            {
                lock (_sync)
                {
                    var meter = instrument.Meter;
                    ref var dict = ref CollectionsMarshal.GetValueRefOrAddDefault(_instrument2State, meter,
                            out var exists);

                    if (!exists)
                    {
                        dict = new Dictionary<Instrument, Measurement>();
                    }
                    
                    var state = Measurement.Build(instrument);
                    dict!.Add(instrument, state);

                    _table.AddRow(new Text(meter.Name.Replace("Paprika.", "")), 
                        new Text(instrument.Name), 
                        state,
                        new Text(instrument.Unit!));
                    
                    listener.EnableMeasurementEvents(instrument, state);    
                }
            },
            MeasurementsCompleted = (instrument, cookie) =>
            {
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
        
        _listener.SetMeasurementEventCallback<double>((i, m, l, c) => ((Measurement)c!).Update(m, l));
        _listener.SetMeasurementEventCallback<float>((i, m, l, c) => ((Measurement)c!).Update(m, l));
        _listener.SetMeasurementEventCallback<long>((i, m, l, c) => ((Measurement)c!).Update(m, l));
        _listener.SetMeasurementEventCallback<int>((i, m, l, c) => ((Measurement)c!).Update(m, l));
        _listener.SetMeasurementEventCallback<short>((i, m, l, c) => ((Measurement)c!).Update(m, l));
        _listener.SetMeasurementEventCallback<byte>((i, m, l, c) => ((Measurement)c!).Update(m, l));
        _listener.SetMeasurementEventCallback<decimal>((i, m, l, c) => ((Measurement)c!).Update((double)m, l));
    }

    public void Report(int blockNumber)
    {
        
    }

    public void Dispose()
    {
        _listener.Dispose();
    }
}