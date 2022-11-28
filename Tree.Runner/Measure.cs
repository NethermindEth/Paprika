using System.Diagnostics;

namespace Tree.Tests;

class Measure : IDisposable
{
    private readonly string _name;
    private readonly int _count;
    private readonly int _batchSize;
    private readonly Stopwatch _stopwatch;

    public Measure(string name, int count, int batchSize)
    {
        _name = name;
        _count = count;
        _batchSize = batchSize;

        _stopwatch = Stopwatch.StartNew();
    }

    public void Dispose()
    {
        var elapsed = _stopwatch.Elapsed;
        var throughput = (int)(_count / elapsed.TotalSeconds);
        Console.WriteLine(
            $"{_name} of {_count:N} items with batch of {_batchSize} took {elapsed.ToString()} giving a throughput {throughput:N} items/s");
    }
}