using Paprika.Data;

namespace Paprika.Db;

public interface IPageManager : IDisposable, IPageResolver
{
    DbAddress GetAddress(in Page page);

    /// <summary>
    /// Flushes all the mapped pages.
    /// </summary>
    void FlushAllPages();

    void FlushRootPage(in Page rootPage);
}