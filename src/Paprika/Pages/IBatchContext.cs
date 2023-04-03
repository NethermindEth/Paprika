namespace Paprika.Pages;

public interface IBatchContext
{
    /// <summary>
    /// Gets the current <see cref="IBatch"/> id.
    /// </summary>
    long BatchId { get; }

    /// <summary>
    /// Gets the page at given address.
    /// </summary>
    Page GetAt(DbAddress address);

    /// <summary>
    /// Gets an unused page that is not clean.
    /// </summary>
    /// <returns></returns>
    Page GetNewPage(out DbAddress addr, bool clear);

    /// <summary>
    /// Gets a writable copy of the page.
    /// </summary>
    /// <param name="page"></param>
    /// <returns></returns>
    Page GetWritableCopy(Page page);
}

/// <summary>
/// The base class for all context implementations.
/// </summary>
abstract class BatchContextBase : IBatchContext
{
    private readonly long _minBatchToPreserve;

    protected BatchContextBase(long batchId, long minBatchToPreserve)
    {
        _minBatchToPreserve = minBatchToPreserve;
        BatchId = batchId;
    }

    public long BatchId { get; }

    public abstract Page GetAt(DbAddress address);

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

        if (page.Header.BatchId < _minBatchToPreserve)
        {
            // no need to preserve this page, can be reused
            AssignBatchId(page);
            return page;
        }
        else
        {
            RegisterForFutureGC(page);
        }

        var @new = GetNewPage(out _, false);
        page.CopyTo(@new);
        AssignBatchId(@new);

        return @new;
    }

    /// <summary>
    /// Registers the given page for future GC.
    /// </summary>
    /// <param name="page">The page to be analyzed and registered for future GC.</param>
    protected abstract void RegisterForFutureGC(Page page);

    /// <summary>
    /// Assigns the batch identifier to a given page, marking it writable by this batch.
    /// </summary>
    protected void AssignBatchId(Page page) => page.Header.BatchId = BatchId;
}