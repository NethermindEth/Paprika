namespace Paprika.Store;

/// <summary>
/// The base class for all context implementations.
/// </summary>
abstract class BatchContextBase : IBatchContext
{
    protected BatchContextBase(uint batchId)
    {
        BatchId = batchId;
    }

    public uint BatchId { get; }

    public abstract Page GetAt(DbAddress address);

    public abstract DbAddress GetAddress(Page page);

    public abstract Page GetNewPage(out DbAddress addr, bool clear);

    /// <summary>
    /// If <paramref name="page"/> is already writable in this batch,
    /// returns the same page. If it's not, it will copy the page and return a new one.
    /// </summary>
    /// <param name="page">The page to be made writable.</param>
    /// <returns>The same page or its copy.</returns>
    public Page GetWritableCopy(Page page)
    {
        if (page.Header.BatchId == BatchId)
            return page;

        RegisterForFutureReuse(page);

        var @new = GetNewPage(out _, false);
        page.CopyTo(@new);
        AssignBatchId(@new);

        return @new;
    }

    public abstract bool WasWritten(DbAddress addr);

    /// <summary>
    /// Registers the given page for future GC.
    /// </summary>
    /// <param name="page">The page to be analyzed and registered for future GC.</param>

    protected abstract void RegisterForFutureReuse(Page page);

    /// <summary>
    /// Assigns the batch identifier to a given page, marking it writable by this batch.
    /// </summary>
    protected void AssignBatchId(Page page)
    {
        page.Header.BatchId = BatchId;
        page.Header.PaprikaVersion = PageHeader.CurrentVersion;
    }
}