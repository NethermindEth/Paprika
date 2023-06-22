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
    ValueTask FlushPages(ICollection<DbAddress> addresses, CommitOptions options);

    ValueTask FlushRootPage(DbAddress rootPage, CommitOptions options);

    /// <summary>
    /// Flushes underlying buffers.
    /// </summary>
    void Flush();
}