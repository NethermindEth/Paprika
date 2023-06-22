using HdrHistogram;

namespace Paprika.Store;

/// <summary>
/// Provides capability to report stats for <see cref="DataPage"/>.
/// </summary>
public interface IReporter
{
    void Report(int level, int emptyBuckets, int filledBuckets, int entriesPerPage);
}

public class StatisticsReporter : IReporter
{
    private readonly Dictionary<int, Level> _levels = new();

    public void Report(int level, int emptyBuckets, int filledBuckets, int entriesPerPage)
    {
        if (_levels.TryGetValue(level, out var lvl) == false)
        {
            lvl = _levels[level] = new Level();
        }

        try
        {
            lvl.ChildCount.RecordValue(filledBuckets);
            lvl.Entries.RecordValue(entriesPerPage);
        }
        catch (Exception e)
        {
            Console.WriteLine(e);
            throw;
        }
    }

    private class Level
    {
        public readonly IntHistogram ChildCount = new(1000, 5);
        public readonly IntHistogram Entries = new(1000, 5);
    }
}