using System.Diagnostics;
using System.Runtime.InteropServices;

namespace Paprika.Store;

/// <summary>
/// Keeps the data of the abandoned list.
/// </summary>
[StructLayout(LayoutKind.Explicit, Size = Size)]
public struct AbandonedList
{
    private const int EntriesStart = DbAddress.Size + sizeof(uint);

    private const int Size = Page.PageSize - PageHeader.Size - RootPage.Payload.AbandonedStart - EntriesStart;
    private const int EntrySize = sizeof(uint) + DbAddress.Size;
    private const int MaxCount = Size / EntrySize;

    [FieldOffset(0)] private DbAddress Current;

    [FieldOffset(4)] private uint EntriesCount;

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

            var i = BatchIds.IndexOfAnyInRange<uint>(1, minBatchId - 1);

            if (i > -1 && minBatchId > 2)
            {
                var at = Addresses[i];

                Debug.Assert(at.IsNull == false);

                Current = at;
                var page = new AbandonedPage(batch.GetAt(at));
                if (page.Next.IsNull)
                {
                    // no next, clear the slot
                    Addresses[i] = default;
                    BatchIds[i] = default;

                    EntriesCount--;
                }
                else
                {
                    Addresses[i] = page.Next;
                }
            }
        }

        if (Current.IsNull)
        {
            reused = default;
            return false;
        }

        var current = new AbandonedPage(batch.GetAt(Current));
        if (current.BatchId != batch.BatchId)
        {
            // The current came from the previous batch.
            // Try to use its own data to copy it over.
            // But first, register it for reuse
            batch.RegisterForFutureReuse(current.AsPage());

            if (current.TryPeek(out var newAt))
            {
                var dest = batch.GetAt(newAt);
                current.CopyTo(dest);
                batch.AssignBatchId(dest);

                current = new AbandonedPage(dest);

                Current = newAt;
                if (current.TryPop(out _) == false)
                {
                    // We getting what was peeked above.
                    throw new Exception("The page cannot be empty! It was just allocated to!");
                }
            }
            else
            {
                // The page is registered for reuse, retry get
                Current = DbAddress.Null;
                return TryGet(out reused, minBatchId, batch);
            }
        }

        Debug.Assert(current.BatchId == batch.BatchId);

        if (current.TryPop(out reused))
        {
            return true;
        }

        // nothing in the current, use current as it has been COWed already
        reused = batch.GetAddress(current.AsPage());
        Current = DbAddress.Null;
        return true;
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

            new AbandonedPage(batch.GetAt(head)).AttachTail(Addresses[maxAt], batch);
            BatchIds[maxAt] = batch.BatchId;
        }
        else
        {
            // find first 0th and store there
            var at = BatchIds.IndexOf(0u);

            Debug.Assert(Addresses[at] == DbAddress.Null);

            BatchIds[at] = batch.BatchId;
            Addresses[at] = head;

            EntriesCount++;
        }
    }

    public void Accept(IPageVisitor visitor, IPageResolver resolver)
    {
        foreach (var addr in Addresses)
        {
            if (addr.IsNull == false)
            {
                var abandoned = new AbandonedPage(resolver.GetAt(addr));
                visitor.On(abandoned, addr);

                abandoned.Accept(visitor, resolver);
            }
        }
    }
}