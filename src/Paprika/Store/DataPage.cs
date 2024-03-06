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

    public static DataPage Wrap(Page page) => new(page);

    private const int BucketCount = 16;

    public ref PageHeader Header => ref page.Header;

    public ref Payload Data => ref Unsafe.AsRef<Payload>(page.Payload);

    public Page Set(in NibblePath key, in ReadOnlySpan<byte> data, IBatchContext batch)
    {
        if (Header.BatchId != batch.BatchId)
        {
            // the page is from another batch, meaning, it's readonly. Copy
            var writable = batch.GetWritableCopy(page);
            return new DataPage(writable).Set(key, data, batch);
        }

        var map = new SlottedArray(Data.DataSpan);
        var isDelete = data.IsEmpty;

        if (isDelete)
        {
            // If it's a deletion and a key is empty or there's no child page, delete in-situ
            if (key.IsEmpty || Data.Buckets[key.FirstNibble].IsNull)
            {
                // Empty key can be deleted only in-situ
                map.Delete(key);
                return page;
            }
        }

        // Try to write in the map
        if (map.TrySet(key, data))
        {
            return page;
        }

        // Find most frequent nibble
        var nibble = FindMostFrequentNibble(map);

        // Try get the child page
        ref var address = ref Data.Buckets[nibble];
        Page child;

        if (address.IsNull)
        {
            // Create child as leaf page
            child = batch.GetNewPage(out address, true);
            child.Header.PageType = PageType.Leaf;
            child.Header.Level = (byte)(Header.Level + 1);
        }
        else
        {
            // The child page is not-null, retrieve it
            child = batch.GetAt(address);
        }

        child = FlushDown(map, nibble, child, batch);
        address = batch.GetAddress(child);


        // The page has some of the values flushed down, try to add again.
        return Set(key, data, batch);
    }

    public int CapacityLeft => Map.CapacityLeft;

    private static Page FlushDown(in SlottedArray map, byte nibble, Page destination, IBatchContext batch)
    {
        foreach (var item in map.EnumerateAll())
        {
            var key = item.Key;
            if (key.IsEmpty) // empty keys are left in page
                continue;

            if (key.FirstNibble != nibble)
                continue;

            var sliced = key.SliceFrom(ConsumedNibbles);

            destination = destination.Header.PageType == PageType.Leaf
                ? new LeafPage(destination).Set(sliced, item.RawData, batch)
                : new DataPage(destination).Set(sliced, item.RawData, batch);

            // Use the special delete for the item that is much faster than map.Delete(item.Key);
            map.Delete(item);
        }

        return destination;
    }

    private static byte FindMostFrequentNibble(SlottedArray map)
    {
        const int count = SlottedArray.BucketCount;

        Span<ushort> stats = stackalloc ushort[count];

        map.GatherCountStatistics(stats);

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

    /// <summary>
    /// Represents the data of this data page. This type of payload stores data in 16 nibble-addressable buckets.
    /// These buckets is used to store up to <see cref="DataSize"/> entries before flushing them down as other pages
    /// like page split. 
    /// </summary>
    [StructLayout(LayoutKind.Explicit, Size = Size)]
    public struct Payload
    {
        public const int Size = Page.PageSize - PageHeader.Size;
        private const int BucketSize = BucketCount * DbAddress.Size;

        /// <summary>
        /// The size of the raw byte data held in this page. Must be long aligned.
        /// </summary>
        private const int DataSize = Size - BucketSize;

        private const int DataOffset = Size - DataSize;

        /// <summary>
        /// The first field of buckets.
        /// </summary>
        [FieldOffset(0)] private DbAddress Bucket;

        public Span<DbAddress> Buckets => MemoryMarshal.CreateSpan(ref Bucket, BucketCount);

        /// <summary>
        /// The first item of map of frames to allow ref to it.
        /// </summary>
        [FieldOffset(DataOffset)] private byte DataStart;

        /// <summary>
        /// Writable area.
        /// </summary>
        public Span<byte> DataSpan => MemoryMarshal.CreateSpan(ref DataStart, DataSize);
    }

    public bool TryGet(scoped NibblePath key, IReadOnlyBatchContext batch, out ReadOnlySpan<byte> result)
    {
        batch.AssertRead(Header);

        // read in-page
        var map = Map;

        // try regular map
        if (map.TryGet(key, out result))
        {
            return true;
        }

        if (key.IsEmpty) // empty keys are left in page
        {
            return false;
        }

        var selected = key.FirstNibble;
        var bucket = Data.Buckets[selected];

        // non-null page jump, follow it!
        if (bucket.IsNull == false)
        {
            var sliced = key.SliceFrom(1);
            var child = batch.GetAt(bucket);
            return child.Header.PageType == PageType.Leaf
                ? new LeafPage(child).TryGet(sliced, batch, out result)
                : new DataPage(child).TryGet(sliced, batch, out result);
        }

        result = default;
        return false;
    }

    private SlottedArray Map => new(Data.DataSpan);

    public void Report(IReporter reporter, IPageResolver resolver, int level)
    {
        var emptyBuckets = 0;

        foreach (var bucket in Data.Buckets)
        {
            if (bucket.IsNull)
            {
                emptyBuckets++;
            }
            else
            {
                var child = resolver.GetAt(bucket);
                if (child.Header.PageType == PageType.Leaf)
                    new LeafPage(child).Report(reporter, resolver, level + 1);
                else
                    new DataPage(child).Report(reporter, resolver, level + 1);
            }
        }

        var slotted = new SlottedArray(Data.DataSpan);

        reporter.ReportDataUsage(Header.PageType, level, BucketCount - emptyBuckets, slotted.Count,
            slotted.CapacityLeft);
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