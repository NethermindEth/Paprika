using System.Diagnostics;
using System.Reflection.PortableExecutable;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Paprika.Data;

using static Paprika.Merkle.Node;

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

    public static DataPage Wrap(Page page) => Unsafe.As<Page, DataPage>(ref page);

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

        // No place in map, try flush to leafs first
        TryFlushDownToExistingChildren(map, batch);

        // Try to write again in the map
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

    private void TryFlushDownToExistingChildren(in SlottedArray map, IBatchContext batch)
    {
        var anyChildren = false;

        Span<Page> children = stackalloc Page[BucketCount];

        for (var i = 0; i < BucketCount; i++)
        {
            var addr = Data.Buckets[i];
            if (addr.IsNull == false)
            {
                var child = batch.GetAt(addr);
                var type = child.Header.PageType;
                if (type is PageType.Leaf or PageType.Standard)
                {
                    children[i] = child;
                    anyChildren = true;
                }
            }
        }

        if (anyChildren == false)
            return;

        foreach (var item in map.EnumerateAll())
        {
            var key = item.Key;
            if (key.IsEmpty) // empty keys are left in page
                continue;

            var i = key.FirstNibble;

            ref var child = ref children[i];
            var childExist = child.Raw != UIntPtr.Zero;

            if (childExist)
            {
                var sliced = key.SliceFrom(ConsumedNibbles);

                Page @new;
                if (child.Header.PageType == PageType.Leaf)
                {
                    var leaf = new LeafPage(child);

                    var (copied, cow) = leaf.TrySet(sliced, item.RawData, batch);
                    if (copied)
                    {
                        map.Delete(item);
                    }

                    @new = cow;
                }
                else
                {
                    var data = new DataPage(child);
                    @new = data.Set(sliced, item.RawData, batch);
                }

                // Check if the page requires the update, if yes, update
                if (!@new.Equals(child.AsPage()))
                {
                    child = @new;
                    Data.Buckets[i] = batch.GetAddress(@new);
                }
            }
        }
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

    public bool TryGet(IReadOnlyBatchContext batch, scoped in NibblePath key, out ReadOnlySpan<byte> result)
        => TryGet(batch, key, out result, this);

    private static bool TryGet(IReadOnlyBatchContext batch, scoped in NibblePath key, out ReadOnlySpan<byte> result, DataPage page)
    {
        var returnValue = false;
        var sliced = key;
        do
        {
            batch.AssertRead(page.Header);
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

            var bucket = page.Data.Buckets[sliced.FirstNibble];
            if (bucket.IsNull)
            {
                break;
            }

            // non-null page jump, follow it!
            sliced = sliced.SliceFrom(1);
            var child = batch.GetAt(bucket);
            if (child.Header.PageType == PageType.Leaf)
            {
                return Unsafe.As<Page, LeafPage>(ref child).TryGet(batch, sliced, out result);
            }

            page = Unsafe.As<Page, DataPage>(ref child);
        } while (true);

        return returnValue;
    }

    private SlottedArray Map => new(Data.DataSpan);

    public void Report(IReporter reporter, IPageResolver resolver, int pageLevel, int trimmedNibbles)
    {
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

        var slotted = new SlottedArray(Data.DataSpan);
        reporter.ReportDataUsage(Header.PageType, pageLevel, trimmedNibbles, slotted);
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