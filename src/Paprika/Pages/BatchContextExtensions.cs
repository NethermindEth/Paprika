namespace Paprika.Pages;

/// <summary>
/// The shared functionality between all the <see cref="IBatchContext"/> implementations.
/// </summary>
public static class BatchContextExtensions
{
    /// <summary>
    /// If <paramref name="page"/> is already writable in this batch,
    /// returns the same page. If it's not, it will copy the page and return a new one.
    /// </summary>
    /// <param name="context">The context to work within.</param>
    /// <param name="page">The page to be made writable.</param>
    /// <returns>The same page or its copy.</returns>
    public static Page GetWritableCopy(this IBatchContext context, Page page)
    {
        if (page.Header.BatchId == context.BatchId)
            return page;

        var @new = context.GetNewPage(out _, false);
        page.CopyTo(@new);
        context.AssignTxId(@new);

        // TODO: the previous page is dangling and the only information it has is the tx_id, mem management is needed.
        // Or a process that would scan pages for being old enough to be reused

        return @new;
    }

    /// <summary>
    /// Assigns the batch identifier to a given page, marking it writable by this batch.
    /// </summary>
    public static void AssignTxId(this IBatchContext context, Page page) => page.Header.BatchId = context.BatchId;
}