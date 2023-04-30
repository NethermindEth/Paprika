namespace Paprika.Pages;

public interface IBatchContext : IReadOnlyBatchContext
{
    /// <summary>
    /// Get the address of the given page.
    /// </summary>
    DbAddress GetAddress(Page page);

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

    /// <summary>
    /// Checks whether the given page was written in this batch.
    /// </summary>
    /// <param name="address"></param>
    /// <returns></returns>
    bool WrittenThisBatch(DbAddress address);
}

public interface IReadOnlyBatchContext : IPageResolver
{
    /// <summary>
    /// Gets the current <see cref="IBatch"/> id.
    /// </summary>
    uint BatchId { get; }
}

public interface IPageResolver
{
    /// <summary>
    /// Gets the page at given address.
    /// </summary>
    Page GetAt(DbAddress address);
}