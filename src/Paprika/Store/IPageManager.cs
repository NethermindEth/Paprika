using Paprika.Data;

namespace Paprika.Store;

public interface IPageManager : IDisposable, IPageResolver
{
    DbAddress GetAddress(in Page page);

    /// <summary>
    /// Gets the page for writing purposes.
    /// </summary>
    Page GetAtForWriting(DbAddress address);

    /// <summary>
    /// Flushes all the mapped pages.
    /// </summary>
    /// <param name="addresses"></param>
    /// <param name="options"></param>
    void FlushPages(IReadOnlyCollection<DbAddress> addresses, CommitOptions options);

    void FlushRootPage(DbAddress rootPage, CommitOptions options);
}