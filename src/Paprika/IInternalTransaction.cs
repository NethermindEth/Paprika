using Paprika.Pages;

namespace Paprika;

public interface IInternalTransaction
{
    /// <summary>
    /// Gets the page at given address.
    /// </summary>
    Page GetAt(int address);

    /// <summary>
    /// Get the address of the given page.
    /// </summary>
    int GetAddress(in Page page);

    /// <summary>
    /// Gets an unused page that is not clean.
    /// </summary>
    /// <returns></returns>
    Page GetNewDirtyPage(out int addr);
}
