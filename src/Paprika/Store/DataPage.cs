using System.Buffers;
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
            // delete locally
            if (LeafCount <= MaxLeafCount)
            {
                map.Delete(key.RawSpan);
                for (var i = 0; i < MaxLeafCount; i++)
                {
                    // TODO: consider checking whether the array contains the data first,
                    // only then make it writable as it results in a COW
                    if (TryGetWritableLeaf(i, batch, out var leaf)) leaf.Delete(key.RawSpan);
                }

                return page;
            }

            var childPageAddress = Data.Buckets[key.FirstNibble];
            if (childPageAddress.IsNull)
            {
                // there's no lower level, delete in map
                map.Delete(key.RawSpan);
                return page;
            }
        }

        // try write in map
        if (map.TrySet(key.RawSpan, data))
        {
            return page;
        }

        // if no Descendants, create first leaf
        if (LeafCount == 0)
        {
            TryGetWritableLeaf(0, batch, out var leaf, true);
            this.LeafCount = 1;
        }

        if (LeafCount <= MaxLeafCount)
        {
            // try get the newest
            TryGetWritableLeaf(LeafCount - 1, batch, out var newest);

            // move as many as possible to the first leaf and try to re-add
            var anyMoved = map.MoveTo(newest) > 0;

            if (anyMoved && map.TrySet(key.RawSpan, data))
            {
                return page;
            }

            this.LeafCount += 1;

            if (LeafCount <= MaxLeafCount)
            {
                // still within leafs count
                TryGetWritableLeaf(LeafCount - 1, batch, out newest, true);

                map.MoveTo(newest);
                if (map.TrySet(key.RawSpan, data))
                {
                    return page;
                }

                Debug.Fail("Shall never enter here as new entries are copied to the map");
                return page;
            }

            // copy leafs and clear the buckets as they will be used by child pages now
            Span<DbAddress> leafs = stackalloc DbAddress[MaxLeafCount];
            Data.Buckets.Slice(0, MaxLeafCount).CopyTo(leafs);
            Data.Buckets.Clear();

            // need to deep copy the page, first memoize the map which has the newest data
            var bytes = ArrayPool<byte>.Shared.Rent(Data.DataSpan.Length);
            var copy = bytes.AsSpan(0, Data.DataSpan.Length);
            Data.DataSpan.CopyTo(copy);

            // clear the map
            Data.DataSpan.Clear();

            // as oldest go first, iterate in the same direction
            foreach (var leaf in leafs)
            {
                var leafPage = batch.GetAt(leaf);
                batch.RegisterForFutureReuse(leafPage);
                var leafMap = GetLeafSlottedArray(leafPage);

                foreach (var item in leafMap.EnumerateAll())
                {
                    Set(NibblePath.FromKey(item.Key), item.RawData, batch);
                }
            }

            foreach (var item in new SlottedArray(copy).EnumerateAll())
            {
                Set(NibblePath.FromKey(item.Key), item.RawData, batch);
            }

            ArrayPool<byte>.Shared.Return(bytes);

            // set the actual data
            return Set(key, data, batch);
        }

        // Find most frequent nibble
        var nibble = FindMostFrequentNibble(map);

        // try get the child page
        ref var address = ref Data.Buckets[nibble];
        Page child;

        if (address.IsNull)
        {
            // create child as the same type as the parent
            child = batch.GetNewPage(out Data.Buckets[nibble], true);
            child.Header.PageType = Header.PageType;
            child.Header.Level = (byte)(Header.Level + 1);
        }
        else
        {
            // the child page is not-null, retrieve it
            child = batch.GetAt(address);
        }

        var dataPage = new DataPage(child);

        dataPage = FlushDown(map, nibble, dataPage, batch);
        address = batch.GetAddress(dataPage.AsPage());

        // The page has some of the values flushed down, try to add again.
        return Set(key, data, batch);
    }

    private DataPage FlushDown(in SlottedArray map, byte nibble, DataPage destination, IBatchContext batch)
    {
        foreach (var item in map.EnumerateAll())
        {
            var key = NibblePath.FromKey(item.Key);
            if (key.IsEmpty) // empty keys are left in page
                continue;

            key = key.SliceFrom(TreeLevelOddity); // for odd levels, slice by 1
            if (key.IsEmpty)
                continue;

            if (key.FirstNibble != nibble)
                continue;

            var sliced = key.SliceFrom(1);
            if (sliced.IsEmpty)
                continue;

            destination = new DataPage(destination.Set(sliced, item.RawData, batch));

            // use the special delete for the item that is much faster than map.Delete(item.Key);
            map.Delete(item);
        }

        return destination;
    }

    private ref byte LeafCount => ref Header.Metadata;
    private const byte MaxLeafCount = 6;

    private bool TryGetWritableLeaf(int index, IBatchContext batch, out SlottedArray leaf,
        bool allocateOnMissing = false)
    {
        ref var addr = ref Data.Buckets[index];

        Page page;

        if (addr.IsNull)
        {
            if (!allocateOnMissing)
            {
                leaf = default;
                return false;
            }

            page = batch.GetNewPage(out addr, true);
            page.Header.PageType = PageType.Leaf;
            page.Header.Level = 0;
        }
        else
        {
            page = batch.GetAt(addr);
        }

        // ensure writable
        if (page.Header.BatchId != batch.BatchId)
        {
            page = batch.GetWritableCopy(page);
            addr = batch.GetAddress(page);
        }

        leaf = GetLeafSlottedArray(page);
        return true;
    }

    private static SlottedArray GetLeafSlottedArray(Page page) => new(new Span<byte>(page.Payload, Payload.Size));

    private int TreeLevelOddity => Header.Level % 2;


    private byte FindMostFrequentNibble(SlottedArray map)
    {
        const int count = SlottedArray.BucketCount;

        Span<ushort> stats = stackalloc ushort[count];

        if (TreeLevelOddity == 0)
        {
            map.GatherCountStatistics(stats, static span =>
            {
                var path = NibblePath.FromKey(span);
                return path.Length > 0 ? path.FirstNibble : byte.MaxValue;
            });
        }
        else
        {
            map.GatherCountStatistics(stats, static span =>
            {
                var path = NibblePath.FromKey(span);
                return path.Length > 1 ? path.GetAt(1) : byte.MaxValue;
            });
        }

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

    public bool TryGet(scoped NibblePath key, IPageResolver batch, out ReadOnlySpan<byte> result)
    {
        // read in-page
        var map = Map;

        // try regular map
        if (map.TryGet(key.RawSpan, out result))
        {
            return true;
        }

        if (LeafCount is > 0 and <= MaxLeafCount)
        {
            // start with the oldest
            for (var i = LeafCount - 1; i >= 0; i--)
            {
                var leafMap = GetLeafSlottedArray(batch.GetAt(Data.Buckets[i]));
                if (leafMap.TryGet(key.RawSpan, out result))
                    return true;
            }

            result = default;
            return false;
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
            var child = new DataPage(batch.GetAt(bucket));
            return child.TryGet(key.SliceFrom(1), batch, out result);
        }

        result = default;
        return false;
    }

    private SlottedArray Map => new(Data.DataSpan);

    public void Report(IReporter reporter, IPageResolver resolver, int level)
    {
        var emptyBuckets = 0;

        if (LeafCount <= MaxLeafCount)
        {
            foreach (var leaf in Data.Buckets.Slice(0, LeafCount))
            {
                var page = resolver.GetAt(leaf);
                var leafMap = GetLeafSlottedArray(page);

                // foreach (var item in leafMap.EnumerateAll())
                // {
                //     //reporter.ReportItem(new StoreKey(item.Key), item.RawData);
                // }

                reporter.ReportDataUsage(page.Header.PageType, level + 1, 0, leafMap.Count,
                    leafMap.CapacityLeft);
            }

            emptyBuckets = BucketCount - LeafCount;
        }
        else
        {
            foreach (var bucket in Data.Buckets)
            {
                if (bucket.IsNull)
                {
                    emptyBuckets++;
                }
                else
                {
                    new DataPage(resolver.GetAt(bucket)).Report(reporter, resolver, level + 1);
                }
            }
        }

        var slotted = new SlottedArray(Data.DataSpan);

        // foreach (var item in slotted.EnumerateAll())
        // {
        //     // reporter.ReportItem(new StoreKey(item.Key), item.RawData);
        // }

        reporter.ReportDataUsage(Header.PageType, level, BucketCount - emptyBuckets, slotted.Count,
            slotted.CapacityLeft);
    }
}