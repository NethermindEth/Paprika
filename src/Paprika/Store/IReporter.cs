using System.Runtime.InteropServices;
using HdrHistogram;

namespace Paprika.Store;

/// <summary>
/// Provides capability to report stats for <see cref="DataPage"/>.
/// </summary>
public interface IReporter
{
    void ReportDataUsage(int level, int filledBuckets, int entriesPerPage);

    /// <summary>
    /// Reports how many batches ago the page was updated.
    /// </summary>
    void ReportPage(uint ageInBatches, PageType type);
}

public class StatisticsReporter : IReporter
{
    public readonly SortedDictionary<int, Level> Levels = new();
    public readonly Dictionary<PageType, int> PageTypes = new();
    public int PageCount = 0;

    public readonly IntHistogram PageAge = new(1_000_000_000, 5);

    public void ReportDataUsage(int level, int filledBuckets, int entriesPerPage)
    {
        if (Levels.TryGetValue(level, out var lvl) == false)
        {
            lvl = Levels[level] = new Level();
        }

        PageCount++;

        lvl.ChildCount.RecordValue(filledBuckets);
        lvl.Entries.RecordValue(entriesPerPage);
    }

    public void ReportPage(uint ageInBatches, PageType type)
    {
        PageAge.RecordValue(ageInBatches);
        var value = PageTypes.GetValueOrDefault(type);
        PageTypes[type] = value + 1;
    }

    public class Level
    {
        public readonly IntHistogram ChildCount = new(1000, 5);
        public readonly IntHistogram Entries = new(1000, 5);
    }
}