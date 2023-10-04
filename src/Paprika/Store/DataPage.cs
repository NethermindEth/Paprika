using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text.Json.Serialization.Metadata;
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
public readonly unsafe struct DataPage : IPage
{
    private const int BucketCount = 16;

    private readonly Page _page;

    [DebuggerStepThrough]
    public DataPage(Page page) => _page = page;

    public ref PageHeader Header => ref _page.Header;

    public ref Payload Data => ref Unsafe.AsRef<Payload>(_page.Payload);

    private const int NibbleCount = 1;

    /// <summary>
    /// Sets values for the given <see cref="SetContext.Key"/>
    /// </summary>
    /// <returns>
    /// The actual page which handled the set operation. Due to page being COWed, it may be a different page.
    /// </returns>
    //TODO: [SkipLocalsInit]
    public Page Set(in SetContext ctx)
    {
        var size = StoreKey.GetMaxByteSize(ctx.Key);
        var key = StoreKey.Encode(ctx.Key, stackalloc byte[size]);
        return Set(NibblePath.FromKey(key.Payload), ctx.Data, ctx.Batch);
    }

    public Page Set(in NibblePath key, in ReadOnlySpan<byte> data, IBatchContext batch)
    {
        if (Header.BatchId != batch.BatchId)
        {
            // the page is from another batch, meaning, it's readonly. Copy
            var writable = batch.GetWritableCopy(_page);

            return new DataPage(writable).Set(key, data, batch);
        }

        var map = new SlottedArray(Data.DataSpan);
        var isDelete = data.IsEmpty;

        if (isDelete)
        {
            if (key.Length == 0)
            {
                // path cannot be held on a lower level so delete in here
                return DeleteInMap(key, map);
            }

            // path is not empty, so it might have a child page underneath with data, let's try
            var childPageAddress = Data.Buckets[key.FirstNibble];
            if (childPageAddress.IsNull)
            {
                // there's no lower level, delete in map
                return DeleteInMap(key, map);
            }
        }

        // try write in map
        if (map.TrySet(key.RawSpan, data))
        {
            return _page;
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

            // 012345
            //  12345
            // A12345
            // B12345
            // Header.Level == 1 && path == 12345

            // TODO: minimal setup + slicedpath.

            destination = new DataPage(destination.Set(sliced, item.RawData, batch));

            // use the special delete for the item that is much faster than map.Delete(item.Key);
            map.Delete(item);
        }

        return destination;
    }

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

    private Page DeleteInMap(in NibblePath key, SlottedArray map)
    {
        map.Delete(key.RawSpan);
        if (map.Count == 0 && Data.Buckets.IndexOfAnyExcept(DbAddress.Null) == -1)
        {
            // map is empty, buckets are empty, page is empty
            // TODO: for now, leave as is 
        }

        return _page;
    }

    /// <summary>
    /// Represents the data of this data page. This type of payload stores data in 16 nibble-addressable buckets.
    /// These buckets is used to store up to <see cref="DataSize"/> entries before flushing them down as other pages
    /// like page split. 
    /// </summary>
    [StructLayout(LayoutKind.Explicit, Size = Size)]
    public struct Payload
    {
        private const int Size = Page.PageSize - PageHeader.Size;
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

    public bool TryGet(Key key, IReadOnlyBatchContext batch, out ReadOnlySpan<byte> result)
    {
        var k = StoreKey.Encode(key, stackalloc byte[StoreKey.GetMaxByteSize(key)]);
        return TryGet(NibblePath.FromKey(k.Payload), batch, out result);
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

        if (key.Length == 0)
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

        var slotted = new SlottedArray(Data.DataSpan);

        foreach (var item in slotted.EnumerateAll())
        {
            reporter.ReportItem(new StoreKey(item.Key), item.RawData);
        }

        reporter.ReportDataUsage(Header.PageType, level, BucketCount - emptyBuckets, slotted.Count,
            slotted.CapacityLeft);
    }
}