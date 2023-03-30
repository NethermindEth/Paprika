namespace Paprika.Pages;

public interface IBatchContext
{
    /// <summary>
    /// Gets the page at given address.
    /// </summary>
    Page GetAt(DbAddress address);

    /// <summary>
    /// Get the address of the given page.
    /// </summary>
    DbAddress GetAddress(in Page page);

    /// <summary>
    /// Gets an unused page that is not clean.
    /// </summary>
    /// <returns></returns>
    Page GetNewDirtyPage(out DbAddress addr);

    long BatchId { get; }

    Page GetWritableCopy(Page page);
}
