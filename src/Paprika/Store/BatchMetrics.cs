namespace Paprika.Store;

/// <summary>
/// The batch is accessed from a single thread, no need to use atomic.
/// </summary>
class BatchMetrics
{
    /// <summary>
    /// The number of pages reused from previously existing in the database.
    /// </summary>
    public int PagesReused { get; set; }

    /// <summary>
    /// The number of newly allocated pages.
    /// </summary>
    public int PagesAllocated { get; set; }

    public int Writes { get; set; }

    public int Reads { get; set; }

    /// <summary>
    /// The page was written this batch and is <see cref="IBatchContext.RegisterForFutureReuse"/>
    /// </summary>
    public int RegisteredToReuseAfterWritingThisBatch { get; set; }
}
