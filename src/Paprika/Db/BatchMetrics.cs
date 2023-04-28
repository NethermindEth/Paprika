namespace Paprika.Db;

class BatchMetrics : IBatchMetrics
{
    public int PagesReused { get; private set; }
    public int PagesAllocated { get; private set; }
    public int UnusedPoolFetch { get; private set; }
    public int AbandonedPagesSlotsCount { get; private set; }
    public int AbandonedPagesCount { get; set; }

    public void ReportPageReused() => PagesReused++;

    public void ReportNewPageAllocation() => PagesAllocated++;

    public void ReportUnusedPoolFetch() => UnusedPoolFetch++;


    public void ReportAbandonedPagesSlotsCount(int abandonedPagesSlotsCount)
    {
        AbandonedPagesSlotsCount = abandonedPagesSlotsCount;
    }
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
    /// How many abandoned pages slots are used in the root page.
    /// </summary>
    int AbandonedPagesSlotsCount { get; }

    /// <summary>
    /// Total pages written during this batch.
    /// </summary>
    int TotalPagesWritten => PagesAllocated + PagesReused;
}

