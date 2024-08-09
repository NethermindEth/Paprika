using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Paprika.Data;

namespace Paprika.Store;

/// <summary>
/// Represents a data page storing account data.
/// </summary>
/// <remarks>
/// The page is capable of storing some data inside of it and provides fan out for lower layers.
/// This means that for small amount of data no creation of further layers is required.
///
/// The page preserves locality of the data though. It's either all the children with a given nibble stored
/// in the parent page, or they are flushed underneath.
/// </remarks>
[method: DebuggerStepThrough]
public readonly unsafe struct DataPage(Page page) : IPageWithData<DataPage>
{
    private const int ConsumedNibbles = 1;
    private const int BucketCount = DbAddressList.Of16.Count;

    public static DataPage Wrap(Page page) => Unsafe.As<Page, DataPage>(ref page);

    public ref PageHeader Header => ref page.Header;

    public ref Payload Data => ref Unsafe.AsRef<Payload>(page.Payload);

    public Page Set(in NibblePath key, in ReadOnlySpan<byte> data, IBatchContext batch)
    {
        if (Header.BatchId != batch.BatchId)
        {
            // The page is from another batch, meaning, it's readonly. Copy on write.
            var writable = batch.GetWritableCopy(page);
            var cowed = batch.GetAddress(writable);
            Set(cowed, key, data, batch);
            return writable;
        }

        Set(batch.GetAddress(page), key, data, batch);
        return page;
    }

    [SkipLocalsInit]
    private static void Set(DbAddress at, in NibblePath key, in ReadOnlySpan<byte> data, IBatchContext batch)
    {
        Debug.Assert(at.IsNull == false, "Should be populated by the caller");
        Debug.Assert(batch.WasWritten(at), "Page should have been cowed before");

        var current = at;
        var k = key;

        while (current.IsNull == false)
        {
            var page = batch.GetAt(current);
            Debug.Assert(batch.WasWritten(current));

            ref var payload = ref Unsafe.AsRef<Payload>(page.Payload);

            var map = new SlottedArray(payload.DataSpan);
            if (data.IsEmpty)
            {
                // Empty data means deletion.
                // If it's a deletion and a key is empty or there's no child page, delete in page
                if (k.IsEmpty || payload.Buckets[k.FirstNibble].IsNull)
                {
                    // Empty key or a key with no children can be deleted only in-situ
                    map.Delete(key);
                    break;
                }
            }

            // Try to write through, if key is not empty and there's a child that was written in this batch
            DbAddress childAddr;
            if (k.IsEmpty == false)
            {
                childAddr = payload.Buckets[k.FirstNibble];
                if (childAddr.IsNull == false && batch.WasWritten(childAddr))
                {
                    // Child was written, advance k and update current
                    k = k.SliceFrom(ConsumedNibbles);
                    current = childAddr;
                    continue;
                }
            }

            // Try to write in the map
            if (map.TrySet(k, data))
            {
                // Update happened, return
                break;
            }

            // First, try to flush the existing
            if (TryFindMostFrequentExistingNibble(map, payload.Buckets, out var nibble))
            {
                childAddr = EnsureExistingChildWritable(batch, ref payload, nibble);
                FlushDown(map, nibble, childAddr, batch);

                // Spin one more time
                continue;
            }

            // None of the existing was flushable, find the most frequent one
            nibble = FindMostFrequentNibble(map);

            // Ensure that the child page exists
            childAddr = payload.Buckets[nibble];
            Debug.Assert(childAddr.IsNull,
                "Address should be null. If it wasn't it should be the case that it's found above");

            // Create a child
            var child = batch.GetNewPage(out childAddr, true);
            child.Header.PageType = PageType.Standard;
            child.Header.Level = (byte)(page.Header.Level + ConsumedNibbles);
            payload.Buckets[nibble] = childAddr;

            FlushDown(map, nibble, childAddr, batch);
            // Spin again to try to set.
        }
    }

    private static DbAddress EnsureExistingChildWritable(IBatchContext batch, ref Payload payload, byte nibble)
    {
        var childAddr = payload.Buckets[nibble];

        Debug.Assert(childAddr.IsNull == false, "Should exist");

        batch.GetAt(childAddr);
        batch.EnsureWritableCopy(ref childAddr);
        payload.Buckets[nibble] = childAddr;

        return childAddr;
    }

    private static byte FindMostFrequentNibble(in SlottedArray map)
    {
        const int count = SlottedArray.BucketCount;

        Span<ushort> stats = stackalloc ushort[count];

        map.GatherCountStats1Nibble(stats);

        byte biggestIndex = 0;
        for (byte i = 1; i < count; i++)
        {
            if (stats[i] > stats[biggestIndex])
            {
                biggestIndex = i;
            }
        }

        return biggestIndex;
    }

    private static bool TryFindMostFrequentExistingNibble(in SlottedArray map, in DbAddressList.Of16 children,
        out byte nibble)
    {
        Span<ushort> stats = stackalloc ushort[BucketCount];
        map.GatherCountStats1Nibble(stats);

        byte biggestIndex = 0;
        ushort biggestValue = 0;

        for (byte i = 0; i < BucketCount; i++)
        {
            if (children[i].IsNull == false && stats[i] > biggestValue)
            {
                biggestIndex = i;
                biggestValue = stats[i];
            }
        }

        if (biggestValue > 0)
        {
            nibble = biggestIndex;
            return true;
        }

        nibble = default;
        return false;
    }

    private static void FlushDown(in SlottedArray map, byte nibble, DbAddress child, IBatchContext batch)
    {
        Debug.Assert(batch.WasWritten(child));

        foreach (var item in map.EnumerateNibble(nibble))
        {
            var sliced = item.Key.SliceFrom(ConsumedNibbles);

            Set(child, sliced, item.RawData, batch);

            // Use the special delete for the item that is much faster than map.Delete(item.Key);
            map.Delete(item);
        }
    }

    /// <summary>
    /// Represents the data of this data page. This type of payload stores data in 16 nibble-addressable buckets.
    /// These buckets is used to store up to <see cref="DataSize"/> entries before flushing them down as other pages
    /// like page split.
    /// </summary>
    [StructLayout(LayoutKind.Explicit, Size = Size)]
    public struct Payload
    {
        public const int Size = Page.PageSize - PageHeader.Size;
        private const int BucketSize = DbAddressList.Of16.Size;

        /// <summary>
        /// The size of the raw byte data held in this page. Must be long aligned.
        /// </summary>
        private const int DataSize = Size - BucketSize;

        private const int DataOffset = Size - DataSize;

        [FieldOffset(0)]
        public DbAddressList.Of16 Buckets;

        /// <summary>
        /// The first item of map of frames to allow ref to it.
        /// </summary>
        [FieldOffset(DataOffset)] private byte DataStart;

        /// <summary>
        /// Writable area.
        /// </summary>
        public Span<byte> DataSpan => MemoryMarshal.CreateSpan(ref DataStart, DataSize);
    }

    public bool TryGet(IReadOnlyBatchContext batch, scoped in NibblePath key, out ReadOnlySpan<byte> result)
        => TryGet(batch, key, out result, this);

    private static bool TryGet(IReadOnlyBatchContext batch, scoped in NibblePath key, out ReadOnlySpan<byte> result, DataPage page)
    {
        var returnValue = false;
        var sliced = key;

        do
        {
            batch.AssertRead(page.Header);
            DbAddress bucket = default;
            if (!sliced.IsEmpty)
            {
                // As the CPU does not auto-prefetch across page boundaries
                // Prefetch child page in case we go there next to reduce CPU stalls
                bucket = page.Data.Buckets[sliced.FirstNibble];
                batch.Prefetch(bucket);
            }

            // try regular map
            if (page.Map.TryGet(sliced, out result))
            {
                returnValue = true;
                break;
            }

            if (sliced.IsEmpty) // empty keys are left in page
            {
                break;
            }

            if (bucket.IsNull)
            {
                break;
            }

            // non-null page jump, follow it!
            sliced = sliced.SliceFrom(ConsumedNibbles);
            var child = batch.GetAt(bucket);
            page = Unsafe.As<Page, DataPage>(ref child);
        } while (true);

        return returnValue;
    }

    private SlottedArray Map => new(Data.DataSpan);

    public int CapacityLeft => Map.CapacityLeft;

    public void Report(IReporter reporter, IPageResolver resolver, int pageLevel, int trimmedNibbles)
    {
        resolver.Prefetch(Data.Buckets);

        var slotted = new SlottedArray(Data.DataSpan);
        reporter.ReportDataUsage(Header.PageType, pageLevel, trimmedNibbles, slotted);

        foreach (var bucket in Data.Buckets)
        {
            if (!bucket.IsNull)
            {
                var child = resolver.GetAt(bucket);
                if (child.Header.PageType == PageType.Leaf)
                    new LeafPage(child).Report(reporter, resolver, pageLevel + 1, trimmedNibbles + 1);
                else
                    new DataPage(child).Report(reporter, resolver, pageLevel + 1, trimmedNibbles + 1);
            }
        }
    }

    public void Accept(IPageVisitor visitor, IPageResolver resolver, DbAddress addr)
    {
        using (visitor.On(this, addr))
        {
            foreach (var bucket in Data.Buckets)
            {
                if (bucket.IsNull)
                {
                    continue;
                }

                var child = resolver.GetAt(bucket);
                if (child.Header.PageType == PageType.Leaf)
                    new LeafPage(child).Accept(visitor, resolver, bucket);
                else
                    new DataPage(child).Accept(visitor, resolver, bucket);
            }
        }
    }
}
