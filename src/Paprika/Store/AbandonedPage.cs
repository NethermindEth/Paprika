using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Paprika.Merkle;

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

    public static DbAddress CreateChain(ReadOnlySpan<DbAddress> abandoned, IBatchContext batch)
    {
        DbAddress next = default;

        while (abandoned.IsEmpty == false)
        {
            // TODO: consider not clearing the page
            batch.GetNewPage(out var addr, true);

            var page = new AbandonedPage(batch.GetAt(addr));
            page.Header.PageType = PageType.Abandoned;
            page.Data.Next = next;
            next = addr;

            if (abandoned.Length > Payload.MaxCount)
            {
                page.Data.Count = Payload.MaxCount;
                abandoned[..Payload.MaxCount].CopyTo(page.Data.Abandoned);
                abandoned = abandoned[Payload.MaxCount..];
            }
            else
            {
                // The last chunk
                page.Data.Count = abandoned.Length;
                abandoned.CopyTo(page.Data.Abandoned);

                return next;
            }
        }

        return next;
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