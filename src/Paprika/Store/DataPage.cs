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
public readonly unsafe struct DataPage : IPage
{
    private const int BucketCount = 16;

    private static readonly byte[] NibbleIndexes = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 0xA, 0xB, 0xC, 0xD, 0xE, 0xF];

    private readonly Page _page;

    [DebuggerStepThrough]
    public DataPage(Page page) => _page = page;

    public ref PageHeader Header => ref _page.Header;

    public ref Payload Data => ref Unsafe.AsRef<Payload>(_page.Payload);

    public (bool, Page) TrySet(in NibblePath key, in ReadOnlySpan<byte> data, IBatchContext batch)
    {
        if (Header.BatchId != batch.BatchId)
        {
            // the page is from another batch, meaning, it's readonly. Copy
            var writable = batch.GetWritableCopy(_page);

            return new DataPage(writable).TrySet(key, data, batch);
        }

        var map = new SlottedArray(Data.DataSpan);
        var isDelete = data.IsEmpty;

        if (isDelete)
        {
            var childPageAddress = Data.Buckets[key.FirstNibble];
            if (childPageAddress.IsNull)
            {
                // there's no lower level, delete in map
                map.Delete(key.RawSpan);
                return (true, _page);
            }
        }

        // try write in map
        return (map.TrySet(key.RawSpan, data), _page);
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
            var childPageAddress = Data.Buckets[key.FirstNibble];
            if (childPageAddress.IsNull)
            {
                // there's no lower level, delete in map
                map.Delete(key.RawSpan);
                return _page;
            }
        }

        // try write in map
        if (map.TrySet(key.RawSpan, data))
        {
            return _page;
        }

        Span<ushort> sizes = stackalloc ushort[BucketCount];

        var anyChildren = Data.Buckets.ContainsAnyExcept(DbAddress.Null);
        if (anyChildren == false)
        {
            // If the page has no leafs, select the nibble with the biggest size to create one.
            GatherMapStats(map, sizes);

            var biggest = FindBiggestNibble(sizes);
            var firstChild = FlushDown<SetForcefully>(map, biggest, GetPageForNibble(batch, biggest), batch);

            Data.Buckets[biggest] = batch.GetAddress(firstChild.AsPage());

            // This means that a child was created and there should be enough of space.
            // Try again.
            if (map.TrySet(key.RawSpan, data))
            {
                return _page;
            }
        }

        // Try gently push down to the existing ones
        // Try push down to child pages nibbles that have something to push.
        // Do not cause the further flushes, just try to set. If it fails, it's ok.
        GatherMapStats(map, sizes);

        Span<byte> indexes = stackalloc byte[BucketCount];

        // sort from the biggest one to the smallest
        NibbleIndexes.AsSpan().CopyTo(indexes);
        sizes.Sort(indexes);

        for (int i = BucketCount - 1; i >= 0; i--)
        {
            var nibble = indexes[i];
            var size = sizes[i];

            ref var child = ref Data.Buckets[nibble];
            if (child.IsNull == false && size > 0)
            {
                // The child exist and has some data to be written to.
                // Try to flush it down.
                var childPage = new DataPage(batch.GetAt(child));
                childPage = FlushDown<TrySetStrategy>(map, nibble, childPage, batch);
                child = batch.GetAddress(childPage.AsPage());

                // Something was flushed down, try to set.
                if (map.TrySet(key.RawSpan, data))
                {
                    return _page;
                }
            }
        }

        {
            // Soft flushing failed. Flush fully.
            GatherMapStats(map, sizes);

            var biggest = FindBiggestNibble(sizes);
            var child = FlushDown<SetForcefully>(map, biggest, GetPageForNibble(batch, biggest), batch);
            Data.Buckets[biggest] = batch.GetAddress(child.AsPage());
        }

        // It should just work now with a single lvl recursion.
        return Set(key, data, batch);
    }

    private DataPage GetPageForNibble(IBatchContext batch, byte nibble)
    {
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

        return new DataPage(child);
    }

    private int GatherMapStats(SlottedArray map, Span<ushort> sizes) => map.GatherMapStats(sizes, TreeLevelOddity);

    private static byte FindBiggestNibble(Span<ushort> sizes)
    {
        byte maxI = 0;

        for (byte i = 1; i < BucketCount; i++)
        {
            if (sizes[i] > sizes[maxI])
            {
                maxI = i;
            }
        }

        return maxI;
    }

    private DataPage FlushDown<TSetStrategy>(in SlottedArray map, byte nibble, DataPage destination,
        IBatchContext batch)
        where TSetStrategy : struct, ISetStrategy
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

            var (success, page) = default(TSetStrategy).TrySet(destination, sliced, item.RawData, batch);
            destination = new DataPage(page);

            if (success == false)
                break;

            // use the special delete for the item that is much faster than map.Delete(item.Key);
            map.Delete(item);
        }

        return destination;
    }

    private int TreeLevelOddity => Header.Level % 2;


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

    private bool IsLeaf => Data.Buckets.ContainsAnyExcept(DbAddress.Null);

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
            // reporter.ReportItem(new StoreKey(item.Key), item.RawData);
        }

        reporter.ReportDataUsage(Header.PageType, level, BucketCount - emptyBuckets, slotted.Count,
            slotted.CapacityLeft);
    }
}

internal interface ISetStrategy
{
    /// <summary>
    /// Tries to set the value
    /// </summary>
    (bool, Page) TrySet(DataPage page, in NibblePath key, in ReadOnlySpan<byte> data, IBatchContext batch);
}

file struct SetForcefully : ISetStrategy
{
    public (bool, Page) TrySet(DataPage page, in NibblePath key, in ReadOnlySpan<byte> data, IBatchContext batch) =>
        (true, page.Set(key, data, batch));
}

file struct TrySetStrategy : ISetStrategy
{
    public (bool, Page) TrySet(DataPage page, in NibblePath key, in ReadOnlySpan<byte> data, IBatchContext batch) =>
        page.TrySet(key, data, batch);
}