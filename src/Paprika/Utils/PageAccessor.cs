using System.Diagnostics.CodeAnalysis;
using Paprika.Store;

namespace Paprika.Utils;

public class PageAccessor
{
    private readonly int numPages;
    private List<byte[]> pages;
    private HashSet<int> availablePages;

    public PageAccessor(int pageSize = 4096, int numpages = 10)
    {
        this.numPages = numpages;
        pages = new List<byte[]>(numpages);
        for (int i = 0; i < numPages; i++)
        {
            pages.Add(new byte[pageSize]);
        }
        availablePages = new HashSet<int>(Enumerable.Range(0, numPages));
    }
    public byte[] AcquirePage()
    {
        if (availablePages.Count == 0)
        {
            throw new InvalidOperationException("No available pages in the pool");
        }
        int pageIndex = availablePages.First();
        availablePages.Remove(pageIndex);
        return pages[pageIndex];
    }
    public void ReleasePage(byte[] page)
    {
        int pageIndex = pages.IndexOf(page);
        if (pageIndex == -1)
        {
            throw new ArgumentException("Page not found in the pool");
        }
        if (availablePages.Contains(pageIndex))
        {
            throw new ArgumentException("Page is already released");
        }
        availablePages.Add(pageIndex);
    }
    public void ReleaseAllPages()
    {
        availablePages = new HashSet<int>(Enumerable.Range(0, numPages));
    }
}

