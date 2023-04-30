namespace Paprika.Db;

class BatchMetrics : IBatchMetrics
{
    public int PagesReused { get; private set; }
    public int PagesAllocated { get; private set; }
    public int UnusedPoolFetch { get; private set; }

    public void ReportPageReused() => PagesReused++;

    public void ReportNewPageAllocation() => PagesAllocated++;

    public void ReportUnusedPoolFetch() => UnusedPoolFetch++;
}

public interface IBatchMetrics
{
    /// <summary>
    /// The number of pages reused from previously existing in the database.
    /// </summary>
    int PagesReused { get; }

    /// <summary>
    /// The number of newly allocated pages.
    /// </summary>
    int PagesAllocated { get; }

    /// <summary>
    /// The count of unused pool fetches.
    /// </summary>
    int UnusedPoolFetch { get; }

    /// <summary>
    /// Total pages written during this batch.
    /// </summary>
    int TotalPagesWritten => PagesAllocated + PagesReused;
}
