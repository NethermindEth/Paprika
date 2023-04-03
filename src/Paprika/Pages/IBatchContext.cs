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
}
