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

    /// <summary>
    /// Forces to flush the underlying no matter what flags they are
    /// </summary>
    void ForceFlush();

    /// <summary>
    /// Provides information whether the page manager uses the persistent paging using
    /// actual IO methods (<see cref="RandomAccess"/> and others).
    ///
    /// If it uses paging not based on the actual IO, this means that pages are never flushed manually and
    /// can be altered just like they were in memory. 
    /// </summary>
    bool UsesPersistentPaging { get; }
}
