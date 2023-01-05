namespace Paprika;

public interface ITransaction
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

    /// <summary>
    /// Gets the writable copy (copy-on-write) of the given page.
    /// </summary>
    /// <param name="page"></param>
    /// <param name="addr"></param>
    /// <returns>The new page with the copied content.</returns>
    Page GetWritableCopy(in Page page, out int addr)
    {
        var allocated = GetNewDirtyPage(out addr);
        page.CopyTo(allocated);
        Abandon(page);
        return allocated;
    }

    /// <summary>
    /// Abandons the page, marking it as unused once the transaction commits.
    /// </summary>
    void Abandon(in Page page);
}