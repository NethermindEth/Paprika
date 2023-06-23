using HdrHistogram;

namespace Paprika.Store;

/// <summary>
/// Provides capability to report stats for <see cref="DataPage"/>.
/// </summary>
public interface IReporter
{
    void Report(int level, int filledBuckets, int entriesPerPage);
}

public class StatisticsReporter : IReporter
{
    public readonly SortedDictionary<int, Level> Levels = new();
    public int PageCount = 0;

    public void Report(int level, int filledBuckets, int entriesPerPage)
    {
        if (Levels.TryGetValue(level, out var lvl) == false)
        {
            lvl = Levels[level] = new Level();
        }

        PageCount++;

        lvl.ChildCount.RecordValue(filledBuckets);
        lvl.Entries.RecordValue(entriesPerPage);
    }

    public class Level
    {
        public readonly IntHistogram ChildCount = new(1000, 5);
        public readonly IntHistogram Entries = new(1000, 5);
    }
}