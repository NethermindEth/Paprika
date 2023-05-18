using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Paprika.Data;

namespace Paprika.Store;

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
    /// The next chunk of pages with the same batch id.
    /// </summary>
    public ref DbAddress Next => ref Data.Next;

    private unsafe ref Payload Data => ref Unsafe.AsRef<Payload>(_page.Payload);

    /// <summary>
    /// Gets a snapshot of what pages are held in here.
    /// </summary>
    public ReadOnlySpan<DbAddress> Abandoned => Data.Abandoned;

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
        var newPage = batch.GetNewPage(out var nextAddr, true);

        newPage.Header.TreeLevel = 0;
        newPage.Header.PageType = PageType.Abandoned;
        newPage.Header.PaprikaVersion = PageHeader.CurrentVersion;

        var next = new AbandonedPage(newPage)
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

        public ReadOnlySpan<DbAddress> Abandoned => MemoryMarshal.CreateSpan(ref AbandonedPages, Count);
    }

    public void Accept(IPageVisitor visitor, IPageResolver resolver)
    {
        var addr = Data.Next;
        if (addr.IsNull == false)
        {
            var abandoned = new AbandonedPage(resolver.GetAt(addr));
            visitor.On(abandoned, addr);

            abandoned.Accept(visitor, resolver);
        }
    }
}