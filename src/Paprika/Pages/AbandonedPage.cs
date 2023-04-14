using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace Paprika.Pages;

/// <summary>
/// Represents a set of pages abandoned during the batch with the same <see cref="IBatchContext.BatchId"/>
/// as the page. 
/// </summary>
public readonly struct AbandonedPage : IPage
{
    private readonly Page _page;

    [DebuggerStepThrough]
    public AbandonedPage(Page page) => _page = page;

    public ref uint AbandonedAtBatch => ref Data.AbandonedAtBatchId;

    /// <summary>
    /// The id of the page that this page was written with.
    /// </summary>
    public uint BatchId => _page.Header.BatchId;

    /// <summary>
    /// Gets the number of pages this contains.
    /// </summary>
    public int PageCount => Data.Count;

    /// <summary>
    /// The next chunk of pages with the same batch id.
    /// </summary>
    public ref DbAddress Next => ref Data.Next;

    private unsafe ref Payload Data => ref Unsafe.AsRef<Payload>(_page.Payload);

    /// <summary>
    /// Enqueues the page to the registry of pages to be freed.
    /// </summary>
    public AbandonedPage EnqueueAbandoned(IBatchContext batch, DbAddress thisPageAddress, DbAddress abandoned)
    {
        if (Data.TryEnqueueAbandoned(abandoned))
        {
            return this;
        }

        // no more space, needs another next AbandonedPage
        var next = new AbandonedPage(batch.GetNewPage(out var nextAddr, true))
        {
            AbandonedAtBatch = AbandonedAtBatch
        };

        next.EnqueueAbandoned(batch, nextAddr, abandoned);

        // chain the new to the current
        next.Data.Next = thisPageAddress;

        // return next as this is chained via Next field
        return next;
    }

    /// <summary>
    /// Tries to dequeue a free page.
    /// </summary>
    public bool TryDequeueFree(out DbAddress page) => Data.TryDequeueFree(out page);

    /// <summary>
    /// Tries to dequeue a free page.
    /// </summary>
    public bool TryPeekFree(out DbAddress page) => Data.TryPeekFree(out page);

    [StructLayout(LayoutKind.Explicit, Size = Size)]
    private struct Payload
    {
        private const int Size = Page.PageSize - PageHeader.Size;

        private const int PageAddressesOffset = sizeof(int) + sizeof(uint) + DbAddress.Size;

        private const int MaxCount = (Size - PageAddressesOffset) / DbAddress.Size;

        /// <summary>
        /// The count of pages contained in this page.
        /// </summary>
        [FieldOffset(0)] public int Count;

        /// <summary>
        /// Provides the id of the batch that pages were abandoned at.
        /// </summary>
        /// <remarks>
        /// It's separate from <see cref="Page.Header.BatchId"/> represents the writability of the page. 
        /// </remarks>
        [FieldOffset(sizeof(int))] public uint AbandonedAtBatchId;

        /// <summary>
        /// The address of the next page that contains information about this <see cref="IBatchContext.BatchId"/>
        /// abandoned pages.
        /// </summary>
        [FieldOffset(sizeof(int) + DbAddress.Size)] public DbAddress Next;

        [FieldOffset(PageAddressesOffset)] private DbAddress AbandonedPages;

        /// <summary>
        /// Tries to enqueue an abandoned page, reporting false on page being full.
        /// </summary>
        /// <param name="page"></param>
        /// <returns></returns>
        public bool TryEnqueueAbandoned(DbAddress page)
        {
            if (Count >= MaxCount)
            {
                // no more place here
                return false;
            }

            // put page in place
            Unsafe.Add(ref AbandonedPages, Count) = page;
            Count++;
            return true;
        }

        /// <summary>
        /// Tries to dequeue an available free page.
        /// </summary>
        /// <param name="page">The page retrieved.</param>
        /// <returns>Whether a page was retrieved</returns>
        public bool TryDequeueFree(out DbAddress page)
        {
            if (Count == 0)
            {
                page = DbAddress.Null;
                return false;
            }

            Count--;
            page = Unsafe.Add(ref AbandonedPages, Count);
            return true;
        }

        public bool TryPeekFree(out DbAddress page)
        {
            if (Count == 0)
            {
                page = DbAddress.Null;
                return false;
            }

            page = Unsafe.Add(ref AbandonedPages, Count - 1);
            return true;
        }
    }
}