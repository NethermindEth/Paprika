using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Diagnostics.Contracts;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace Paprika.Store;

/// <summary>
/// Keeps the data of the abandoned list.
/// </summary>
[StructLayout(LayoutKind.Explicit, Size = Size)]
public struct AbandonedList
{
    /// <summary>
    /// The start for spans of <see cref="BatchIds"/> and <see cref="Addresses"/>.
    /// </summary>
    private const int EntriesStart = DbAddress.Size + sizeof(uint);

    public const int Size = Page.PageSize;
    private const int EntrySize = sizeof(uint) + DbAddress.Size;
    private const int MaxCount = (Size - EntriesStart) / EntrySize;

    [FieldOffset(0)] private DbAddress Current;
    [FieldOffset(DbAddress.Size)] private uint EntriesCount;

    [FieldOffset(EntriesStart)] private uint BatchIdStart;

    private Span<uint> BatchIds => MemoryMarshal.CreateSpan(ref BatchIdStart, MaxCount);

    [FieldOffset(MaxCount * sizeof(uint) + EntriesStart)]
    private DbAddress AddressStart;

    private Span<DbAddress> Addresses => MemoryMarshal.CreateSpan(ref AddressStart, MaxCount);

    /// <summary>
    /// Tries to get the reused.
    /// </summary>
    public bool TryGet(out DbAddress reused, uint minBatchId, IBatchContext batch)
    {
        if (Current.IsNull)
        {
            // Try find current
            if (EntriesCount == 0)
            {
                reused = default;
                return false;
            }

            // find first batch matching the range
            var id = BatchIds[0];
            if (minBatchId > 2 && id < minBatchId)
            {
                var at = Addresses[0];

                Debug.Assert(at.IsNull == false);

                Current = at;
                var page = batch.GetAt(at);
                var abandoned = new AbandonedPage(page);
                if (abandoned.Next.IsNull)
                {
                    if (EntriesCount == 1)
                    {
                        // empty all
                        Addresses[0] = default;
                        BatchIds[0] = default;
                        EntriesCount = 0;
                    }
                    else
                    {
                        var resized = (int)EntriesCount - 1;

                        // at least two entries, copy slices to move
                        Addresses.Slice(1, resized).CopyTo(Addresses.Slice(0, resized));
                        BatchIds.Slice(1, resized).CopyTo(BatchIds.Slice(0, resized));

                        Addresses[resized] = default;
                        BatchIds[resized] = default;

                        EntriesCount--;
                    }
                }
                else
                {
                    Addresses[0] = abandoned.Next;
                }
            }
        }

        if (Current.IsNull)
        {
            reused = default;
            return false;
        }

        var pageAt = batch.GetAt(Current);
        var current = new AbandonedPage(pageAt);

        if (current.BatchId != batch.BatchId)
        {
            // The current came from the previous batch.
            // First, register it for reuse
            batch.RegisterForFutureReuse(current.AsPage());

            if (current.TryPeek(out var newAt, out var hasMoreThanPeeked))
            {
                if (hasMoreThanPeeked == false)
                {
                    // Special case as the current has only one page. 
                    // There's no use in COWing the page. Just return the page and clean the current
                    reused = newAt;
                    Current = DbAddress.Null;
                    return true;
                }

                // If current has a child, we can use the child and COW to it
                var dest = batch.GetAt(newAt);
                batch.NoticeAbandonedPageReused(dest);
                current.CopyTo(dest);
                batch.AssignBatchId(dest);

                current = new AbandonedPage(dest);

                Current = newAt;
                if (current.TryPop(out _) == false)
                {
                    // This should pop the one that was Peeked above.
                    ThrowPageEmpty();
                }
            }
            else
            {
                // The page is registered for reuse, retry get
                Current = DbAddress.Null;
                return TryGet(out reused, minBatchId, batch);
            }
        }

        Debug.Assert(current.BatchId == batch.BatchId, "Abandoned page should have been COWed properly");

        if (current.TryPop(out reused))
        {
            return true;
        }

        // Nothing in the current.
        // Register as ready to be reused for sake of bookkeeping and retry the get.
        batch.RegisterForFutureReuse(current.AsPage());
        Current = DbAddress.Null;
        return TryGet(out reused, minBatchId, batch);

        [DoesNotReturn]
        [StackTraceHidden]
        static void ThrowPageEmpty()
        {
            throw new Exception("The page cannot be empty! It was just allocated to!");
        }
    }

    public void Register(List<DbAddress> abandoned, IBatchContext batch)
    {
        var head = AbandonedPage.CreateChain(abandoned, batch);
        Register(head, batch);
    }

    private void Register(DbAddress head, IBatchContext batch)
    {
        if (MaxCount == EntriesCount)
        {
            // No place, find the youngest and attach to it
            var maxAt = 0;

            for (var i = 1; i < MaxCount; i++)
            {
                if (BatchIds[i] > BatchIds[maxAt])
                {
                    maxAt = i;
                }
            }

            // 1. Attach the previously existing abandoned as tail to the current one
            new AbandonedPage(batch.GetAt(head)).AttachTail(Addresses[maxAt], batch);
            // 2. Update the batch id
            BatchIds[maxAt] = batch.BatchId;
            // 3. Set properly the address to the head that has been chained up
            Addresses[maxAt] = head;
        }
        else
        {
            // find first 0th and store there
            var at = (int)EntriesCount;

            Debug.Assert(Addresses[at] == DbAddress.Null);

            BatchIds[at] = batch.BatchId;
            Addresses[at] = head;

            EntriesCount++;
        }
    }

    public void Accept(IPageVisitor visitor, IPageResolver resolver)
    {
        TryAcceptAbandoned(visitor, resolver, Current);

        foreach (var addr in Addresses)
        {
            TryAcceptAbandoned(visitor, resolver, addr);
        }
    }

    private static void TryAcceptAbandoned(IPageVisitor visitor, IPageResolver resolver, DbAddress addr)
    {
        if (addr.IsNull)
            return;

        var abandoned = new AbandonedPage(resolver.GetAt(addr));
        visitor.On(abandoned, addr);
        abandoned.Accept(visitor, resolver);
    }

    [Pure]
    public long GatherTotalAbandoned(IPageResolver resolver)
    {
        resolver.Prefetch(Addresses);

        long count = 0;

        foreach (var addr in Addresses[..(int)EntriesCount])
        {
            var current = addr;
            while (current.IsNull == false)
            {
                var abandoned = new AbandonedPage(resolver.GetAt(current));
                count += abandoned.Count;
                current = abandoned.Next;
            }
        }

        return count;
    }

    public bool IsFullyEmpty
    {
        get
        {
            const int notFound = -1;

            return Addresses.IndexOfAnyExcept(DbAddress.Null) == notFound &&
                   BatchIds.IndexOfAnyExcept(default(uint)) == notFound &&
                   EntriesCount == 0 &&
                   Current == DbAddress.Null;
        }
    }

    public DbAddress GetCurrentForTest() => Current;

    public static ref AbandonedList Wrap(Page page) =>
        ref Unsafe.As<byte, AbandonedList>(ref MemoryMarshal.GetReference(page.Span));
}