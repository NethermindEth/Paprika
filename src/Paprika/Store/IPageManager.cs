using Paprika.Data;

namespace Paprika.Store;

public interface IPageManager : IDisposable, IPageResolver
{
    DbAddress GetAddress(in Page page);

    /// <summary>
    /// Gets the page for writing purposes, ensuring that the page that is requested is finalized on disk.
    /// </summary>
    Page GetAtForWriting(DbAddress address, bool reused);

    /// <summary>
    /// Flushes all the mapped pages.
    /// </summary>
    /// <param name="addresses"></param>
    /// <param name="options"></param>
    void FlushPages(IReadOnlyCollection<DbAddress> addresses, CommitOptions options);

    void FlushRootPage(DbAddress rootPage, CommitOptions options);
}