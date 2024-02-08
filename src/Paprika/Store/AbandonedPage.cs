using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace Paprika.Store;

/// <summary>
/// Represents a set of pages abandoned during the batch with the same <see cref="IBatchContext.BatchId"/>
/// as the page. 
/// </summary>
[method: DebuggerStepThrough]
public readonly struct AbandonedPage(Page page) : IPage
{
    private ref PageHeader Header => ref page.Header;
    public uint BatchId => Header.BatchId;
    public DbAddress Next => Data.Next;

    private unsafe ref Payload Data => ref Unsafe.AsRef<Payload>(page.Payload);

    [StructLayout(LayoutKind.Explicit, Size = Size)]
    private struct Payload
    {
        private const int Size = Page.PageSize - PageHeader.Size;

        private const int PageAddressesOffset = sizeof(int) + DbAddress.Size;

        public const int MaxCount = (Size - PageAddressesOffset) / DbAddress.Size;

        /// <summary>
        /// The count of pages contained in this page.
        /// </summary>
        [FieldOffset(0)] public int Count;

        /// <summary>
        /// The address of the next page that contains information about this <see cref="IBatchContext.BatchId"/>
        /// abandoned pages.
        /// </summary>
        [FieldOffset(sizeof(int))] public DbAddress Next;

        [FieldOffset(PageAddressesOffset)] private DbAddress AbandonedPages;

        public Span<DbAddress> Abandoned => MemoryMarshal.CreateSpan(ref AbandonedPages, MaxCount);
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

    public bool TryPeek(out DbAddress addr)
    {
        if (Data.Count == 0)
        {
            addr = default;
            return false;
        }

        addr = Data.Abandoned[Data.Count - 1];
        return true;
    }

    public bool TryPop(out DbAddress address)
    {
        if (Data.Count == 0)
        {
            address = default;
            return false;
        }

        address = Data.Abandoned[Data.Count - 1];

        Data.Count--;
        return true;
    }

    /// <summary>
    /// Construct the chain of abandoned pages, allowing the abandoned to growth as they are constructed.
    /// </summary>
    /// <param name="abandoned"></param>
    /// <param name="batch"></param>
    /// <returns></returns>
    public static DbAddress CreateChain(List<DbAddress> abandoned, IBatchContext batch)
    {
        var to = 0;

        DbAddress next = default;

        while (to < abandoned.Count)
        {
            // TODO: consider not clearing the page
            batch.GetNewPage(out var addr, true);

            var page = new AbandonedPage(batch.GetAt(addr));
            page.Header.PageType = PageType.Abandoned;
            page.Data.Next = next;
            next = addr;

            var length = abandoned.Count - to;

            if (length > Payload.MaxCount)
            {
                Append(abandoned, page, to, Payload.MaxCount);
                to += Payload.MaxCount;
            }
            else if (length > 0)
            {
                // Append the limited length
                Append(abandoned, page, to, length);
                to += length;
            }
        }

        return next;

        static void Append(List<DbAddress> abandoned, in AbandonedPage page, int to, int length)
        {
            page.Data.Count = length;
            var chunk = CollectionsMarshal.AsSpan(abandoned)
                .Slice(to, length);
            chunk.CopyTo(page.Data.Abandoned);
        }
    }

    public void AttachTail(DbAddress tail, IBatchContext batch)
    {
        Debug.Assert(page.Header.BatchId == batch.BatchId);

        if (Data.Next.IsNull)
        {
            Data.Next = tail;
            return;
        }

        new AbandonedPage(batch.GetAt(Data.Next)).AttachTail(tail, batch);
    }
}