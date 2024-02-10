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
            var childAddr = Data.Buckets[key.FirstNibble];
            if (childAddr.IsNull)
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

        // Find most frequent nibble
        var nibble = FindMostFrequentNibble(map);

        // try get the child page
        ref var address = ref Data.Buckets[nibble];
        Page child;

        if (address.IsNull)
        {
            // Create the leaf page
            child = batch.GetNewPage(out var addr, true);

            Data.Buckets[nibble] = addr;
            child.Header.PageType = PageType.Leaf;
            child.Header.Level = (byte)(Header.Level + 1);
        }
        else
        {
            // the child page is not-null, retrieve it
            child = batch.GetAt(address);
        }

        var flushedTo = FlushDown(map, nibble, child, batch);
        address = batch.GetAddress(flushedTo.AsPage());

        // The page has some of the values flushed down, try to add again.
        return Set(key, data, batch);
    }

    private Page FlushDown(in SlottedArray map, byte nibble, Page dest, IBatchContext batch)
    {
        foreach (var item in map.EnumerateAll())
        {
            var key = NibblePath.FromKey(item.Key);
            if (key.IsEmpty) // empty keys are left in page
                continue;

            key = key.SliceFrom(Header.LevelOddity); // for odd levels, slice by 1
            if (key.IsEmpty)
                continue;

            if (key.FirstNibble != nibble)
                continue;

            var sliced = key.SliceFrom(1);
            if (sliced.IsEmpty)
                continue;

            if (dest.Header.PageType == PageType.Leaf)
            {
                dest = new LeafPage(dest).Set(sliced, item.RawData, batch);
            }
            else
            {
                dest = new DataPage(dest).Set(sliced, item.RawData, batch);
            }

            // use the special delete for the item that is much faster than map.Delete(item.Key);
            map.Delete(item);
        }

        return dest;
    }

    private byte FindMostFrequentNibble(SlottedArray map)
    {
        const int count = SlottedArray.BucketCount;

        Span<ushort> stats = stackalloc ushort[count];

        if (Header.LevelOddity == 0)
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

    public bool TryGet(scoped NibblePath key, IReadOnlyBatchContext batch, out ReadOnlySpan<byte> result)
    {
        batch.AssertRead(Header);

        // read in-page
        var map = Map;

        // try regular map
        if (map.TryGet(key.RawSpan, out result))
        {
            return true;
        }

        if (key.IsEmpty) // empty keys are left in page
        {
            return false;
        }

        var selected = key.FirstNibble;
        var bucket = Data.Buckets[selected];

        // Check bucket
        if (bucket.IsNull == false)
        {
            var child = batch.GetAt(bucket);
            if (child.Header.PageType == PageType.Leaf)
            {
                var leaf = new LeafPage(child);
                return leaf.TryGet(key.SliceFrom(1), batch, out result);
            }

            var data = new DataPage(child);
            return data.TryGet(key.SliceFrom(1), batch, out result);
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
                var resolved = resolver.GetAt(bucket);
                if (resolved.Header.PageType == PageType.Leaf)
                {
                    new LeafPage(resolved).Report(reporter, resolver, level + 1);
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