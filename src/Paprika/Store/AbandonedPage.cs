using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace Paprika.Store;

/// <summary>
/// Represents a set of pages abandoned during the batch with the same <see cref="IBatchContext.BatchId"/>
/// as the page. 
/// </summary>
/// <remarks>
/// The structure uses a packed storage so that consecutive numbers that are different by <see cref="PackedFlag"/>
/// are written over a single <see cref="uint"/> using a flag <see cref="PackedFlag"/>.
/// </remarks>
[method: DebuggerStepThrough]
public readonly struct AbandonedPage(Page page) : IPage
{
    private const uint PackedFlag = 0x8000_0000u;
    private const uint PackedDiff = 1;

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

        [FieldOffset(PageAddressesOffset)] private uint AbandonedPages;

        public Span<uint> Abandoned => MemoryMarshal.CreateSpan(ref AbandonedPages, MaxCount);
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

        var top = Data.Abandoned[Data.Count - 1];

        if ((top & PackedFlag) == PackedFlag)
        {
            // Remove the flag and return
            addr = new DbAddress((top & ~PackedFlag) + PackedDiff);
            return true;
        }

        // Not packed, handle
        addr = new DbAddress(top);
        return true;
    }

    public bool TryPop(out DbAddress addr)
    {
        if (Data.Count == 0)
        {
            addr = default;
            return false;
        }

        ref var top = ref Data.Abandoned[Data.Count - 1];

        if ((top & PackedFlag) == PackedFlag)
        {
            // Remove packed flag
            top &= ~PackedFlag;

            // Return next
            addr = new DbAddress(top + PackedDiff);
            return true;
        }

        // Not packed, handle
        addr = new DbAddress(top);
        Data.Count--;
        return true;
    }

    /// <summary>
    /// Construct the chain of abandoned pages, allowing the abandoned to growth as they are constructed.
    /// </summary>
    public static DbAddress CreateChain(List<DbAddress> abandoned, IBatchContext batch)
    {
        var to = 0;

        DbAddress next = default;

        abandoned.Sort((a, b) => a.Raw.CompareTo(b.Raw));

        while (to < abandoned.Count)
        {
            // TODO: consider not clearing the page
            batch.GetNewPage(out var addr, true);

            var page = new AbandonedPage(batch.GetAt(addr));
            page.Header.PageType = PageType.Abandoned;
            page.Data.Next = next;
            page.Data.Count = 0; // initialize

            next = addr;

            while (to < abandoned.Count && page.TryPush(abandoned[to]))
            {
                to++;
            }
        }

        return next;
    }

    public bool TryPush(DbAddress address)
    {
        Debug.Assert((address.Raw & PackedFlag) == 0u,
            "Database is over 8TB!");

        ref var count = ref Data.Count;

        if (count == Payload.MaxCount)
        {
            return false;
        }

        if (count == 0)
        {
            // Nothing in the page yet, push the first
            Data.Abandoned[count++] = address;
        }
        else
        {
            ref var last = ref Data.Abandoned[count - 1];

            // If last is not packed and last is previous to the address added, pack it.
            if ((last & PackedFlag) == 0 &&
                last + PackedDiff == address)
            {
                last |= PackedFlag;
            }
            else
            {
                Data.Abandoned[count++] = address;
            }
        }

        return true;
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