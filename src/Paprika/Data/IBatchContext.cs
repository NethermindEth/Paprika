using Paprika.Db;

namespace Paprika.Data;

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
    /// Checks whether the page was written during this batch.
    /// </summary>
    bool WasWritten(DbAddress addr);
}

public interface IReadOnlyBatchContext : IPageResolver
{
    /// <summary>
    /// Gets the current <see cref="IBatch"/> id.
    /// </summary>
    uint BatchId { get; }
}

/// <summary>
/// Provides a capability to resolve a page by its page address.
/// </summary>
public interface IPageResolver
{
    /// <summary>
    /// Gets the page at given address.
    /// </summary>
    Page GetAt(DbAddress address);
}