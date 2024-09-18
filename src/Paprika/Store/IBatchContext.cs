using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using Paprika.Crypto;

namespace Paprika.Store;

public interface IBatchContext : IReadOnlyBatchContext
{
    /// <summary>
    /// Get the address of the given page.
    /// </summary>
    DbAddress GetAddress(Page page);

    /// <summary>
    /// Gets an unused page that is not clean.
    /// </summary>
    /// <returns></returns>
    Page GetNewPage(out DbAddress addr, bool clear);

    /// <summary>
    /// Gets a writable copy of the page.
    /// </summary>
    /// <param name="page"></param>
    /// <returns></returns>
    Page GetWritableCopy(Page page);

    Page EnsureWritableCopy(ref DbAddress addr)
    {
        Debug.Assert(addr.IsNull == false);

        var page = GetAt(addr);

        if (page.Header.BatchId == BatchId)
        {
            return page;
        }

        var cow = GetWritableCopy(page);
        addr = GetAddress(cow);
        return cow;
    }

    /// <summary>
    /// Informs the batch, that the given page was abandoned before and is manually reused.
    /// </summary>
    /// <param name="page">The page that will be reused.</param>
    public void NoticeAbandonedPageReused(Page page);

    /// <summary>
    /// Checks whether the page was written during this batch.
    /// </summary>
    bool WasWritten(DbAddress addr);

    /// <summary>
    /// Abandon this page from this batch on.
    /// </summary>
    /// <param name="page">The page to be reused.</param>
    /// <param name="possibleImmediateReuse">If set to true, the page will be checked if it was written in this batch and if it was, will be reused immediately.</param>
    void RegisterForFutureReuse(Page page, bool possibleImmediateReuse = false);

    /// <summary>
    /// Assigns the batch identifier to a given page, marking it writable by this batch.
    /// </summary>
    void AssignBatchId(Page page);

    /// <summary>
    /// Tries to get the page and if it does not exist, allocates one.
    /// </summary>
    /// <param name="addr">The address to check.</param>
    /// <param name="pageType">The page type to assign.</param>
    /// <returns>The page either allocated or get.</returns>
    Page TryGetPageAlloc(ref DbAddress addr, PageType pageType);

    BatchStats? Stats { get; }

    public unsafe Page NullPage => new Page((byte*)0);
}

public class BatchStats : IBatchStats
{
    public int DataPageNewLeafsAllocated { get; private set; }
    public int LeafPageTurnedIntoDataPage { get; private set; }

    public int LeafPageAllocatedOverflows { get; private set; }

    public void DataPageAllocatesNewLeaf() => DataPageNewLeafsAllocated++;

    public void LeafPageTurnsIntoDataPage() => LeafPageTurnedIntoDataPage++;

    public void LeafPageAllocatesOverflows(int count) => LeafPageAllocatedOverflows += count;
}

public interface IReadOnlyBatchContext : IPageResolver
{
    /// <summary>
    /// Gets the current <see cref="IBatch"/> id.
    /// </summary>
    uint BatchId { get; }

    IDictionary<Keccak, uint> IdCache { get; }
}

public static class ReadOnlyBatchContextExtensions
{
    public static void AssertRead(this IReadOnlyBatchContext batch, in PageHeader header)
    {
        if (header.BatchId > batch.BatchId)
        {
            ThrowWrongBatch(header);
        }

        [DoesNotReturn]
        [StackTraceHidden]
        static void ThrowWrongBatch(in PageHeader header)
        {
            throw new Exception($"The page that is at batch {header.BatchId} should not be read by a batch with lower batch number {header.BatchId}.");
        }
    }
}

/// <summary>
/// Provides a capability to resolve a page by its page address.
/// </summary>
public interface IPageResolver
{
    /// <summary>
    /// Gets the page at given address.
    /// </summary>
    Page GetAt(DbAddress address);

    void Prefetch(DbAddress address);

    void Prefetch(ReadOnlySpan<DbAddress> addresses)
    {
        foreach (var bucket in addresses)
        {
            if (!bucket.IsNull)
            {
                Prefetch(bucket);
            }
        }
    }

    void Prefetch<TAddressList>(in TAddressList addresses)
        where TAddressList : struct, DbAddressList.IDbAddressList
    {
        for (var i = 0; i < TAddressList.Length; i++)
        {
            var addr = addresses[i];
            if (!addr.IsNull)
            {
                Prefetch(addr);
            }
        }
    }
}
