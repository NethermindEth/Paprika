using System.Diagnostics;

namespace Tree.Tests;

class Measure : IDisposable
{
    private readonly string _name;
    private readonly int _count;
    private readonly Stopwatch _stopwatch;

    public Measure(string name, int count)
    {
        _name = name;
        _count = count;

        _stopwatch = Stopwatch.StartNew();
    }

    public void Dispose()
    {
        var elapsed = _stopwatch.Elapsed;
        var throughput = (int)(_count / elapsed.TotalSeconds);
        Console.WriteLine(
            $"{_name} of {_count:N} items took {elapsed.ToString()} giving a throughput {throughput:N} items/s");
    }
}