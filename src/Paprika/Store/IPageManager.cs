namespace Paprika.Store;

public interface IPageManager : IDisposable, IPageResolver
{
    /// <summary>
    /// Whether the address does not breach the provided address space.
    /// </summary>
    bool IsValidAddress(DbAddress address);

    DbAddress GetAddress(in Page page);

    /// <summary>
    /// Writes pages specified by <paramref name="addresses"/> to the underlying storage.
    /// </summary>
    /// <remarks>
    /// If <paramref name="options"/> are <see cref="CommitOptions.FlushDataOnly"/> or
    /// <see cref="CommitOptions.FlushDataAndRoot"/>, the write operations are followed by a <see cref="Flush"/>.
    /// This ensures ACI in ACID. 
    /// </remarks>
    ValueTask WritePages(ICollection<DbAddress> addresses, CommitOptions options);

    /// <summary>
    /// Writes <paramref name="pages"/> provided as pairs of address and a page that should be written to the given address.
    /// </summary>
    /// <remarks>
    /// If <paramref name="options"/> are <see cref="CommitOptions.FlushDataOnly"/> or
    /// <see cref="CommitOptions.FlushDataAndRoot"/>, the write operations are followed by a <see cref="Flush"/>.
    /// This ensures ACI in ACID. 
    /// </remarks>
    ValueTask WritePages(IEnumerable<(DbAddress at, Page page)> pages, CommitOptions options);

    /// <summary>
    /// Writes the specified <paramref name="rootPage"/> to the underlying storage.
    /// </summary>
    /// <remarks>
    /// If <paramref name="options"/> are <see cref="CommitOptions.FlushDataAndRoot"/>,
    /// the write operations are followed by a <see cref="Flush"/>.
    ///
    /// This ensures D in ACID.
    ///
    /// If no flush is done, it will be synchronized with a next commit. 
    /// </remarks>
    ValueTask WriteRootPage(DbAddress rootPage, CommitOptions options);

    /// <summary>
    /// Flushes buffers, using FSYNC or FlushFileBuffers.
    /// </summary>
    void Flush();

    /// <summary>
    /// First flushes the underlying view of the file, then, performs <see cref="Flush"/>.
    /// </summary>
    /// <remarks>
    /// This operation should be used only when working with the mem mapped file without issuing
    /// <see cref="WritePages(System.Collections.Generic.ICollection{Paprika.Store.DbAddress},Paprika.CommitOptions)"/>
    /// and <see cref="WriteRootPage"/>, for example in IMPORT scenario.
    /// </remarks>
    void ForceFlush();
}
