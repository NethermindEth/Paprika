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
    Page GetNewPage(out DbAddress addr, bool clear);

    long BatchId { get; }

    /// <summary>
    /// If <paramref name="page"/> is already writable in this batch,
    /// returns the same page. If it's not, it will copy the page and return a new one.
    /// </summary>
    /// <param name="page">The page to be made writable.</param>
    /// <returns>The same page or its copy.</returns>
    Page GetWritableCopy(Page page);
}
