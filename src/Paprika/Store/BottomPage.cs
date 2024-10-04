using System.Buffers;
using System.Collections.Specialized;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Paprika.Data;

namespace Paprika.Store;

/// <summary>
/// One of the bottom pages in the tree.
/// </summary>
[method: DebuggerStepThrough]
public readonly unsafe struct BottomPage(Page page) : IPage, IClearable, IPage<BottomPage>
{
    private ref PageHeader Header => ref page.Header;

    private ref Payload Data => ref Unsafe.AsRef<Payload>(page.Payload);

    [StructLayout(LayoutKind.Explicit, Pack = sizeof(byte), Size = Size)]
    private struct Payload
    {
        private const int Size = Page.PageSize - PageHeader.Size;
        private const int AddressListSize = DbAddressList.Of4.Size;
        public const int BucketCount = DbAddressList.Of4.Count;
        private const int DataSize = Size - AddressListSize;

        [FieldOffset(0)]
        public DbAddressList.Of4 Buckets;

        /// <summary>
        /// The first item of map of frames to allow ref to it.
        /// </summary>
        [FieldOffset(AddressListSize)]
        private byte DataStart;

        /// <summary>
        /// Writable area.
        /// </summary>
        public Span<byte> DataSpan => MemoryMarshal.CreateSpan(ref DataStart, DataSize);
    }

    public SlottedArray Map => new(Data.DataSpan);

    public void Accept(ref NibblePath.Builder builder, IPageVisitor visitor, IPageResolver resolver, DbAddress addr)
    {
        using var scope = visitor.On(ref builder, this, addr);
    }

    public Page Set(in NibblePath key, in ReadOnlySpan<byte> data, IBatchContext batch)
    {
        if (Header.BatchId != batch.BatchId)
        {
            return new BottomPage(batch.GetWritableCopy(page)).Set(key, data, batch);
        }

        var map = Map;

        if (data.IsEmpty)
        {
            // Delete, delete locally and in a child if it exists
            map.Delete(key);

            if (key.IsEmpty == false && TryGetWritableChildAt(batch, GetIndex(key), out var child))
            {
                child.Map.Delete(key);
            }

            return page;
        }

        Debug.Assert(data.IsEmpty == false, "Should be an upsert, not a delete");

        // Try set directly
        if (map.TrySet(key, data))
        {
            return page;
        }

        // Not successful, need to flush down, try set in all existing children first
        FlushToExisting(batch, map);

        // Try set again
        if (map.TrySet(key, data))
        {
            return page;
        }

        // Flushing to existing didn't help. Try to allocate and flush to a new
        if (TryFlushToNew(batch, map))
        {
            // Flush to a new succeeded, try to set.

            if (map.TrySet(key, data))
            {
                return page;
            }
        }

        // Reuse this page for easier management and no need of copying it back in the parent.
        // 1. copy the content
        // 2. reuse the page
        // TODO: replace this with unmanaged pool of Paprika?
        var dataSpan = Data.DataSpan;
        var buffer = ArrayPool<byte>.Shared.Rent(dataSpan.Length);
        var copy = buffer.AsSpan(0, dataSpan.Length);

        dataSpan.CopyTo(copy);
        var children = Data.Buckets.ToArray();

        // All flushing failed, requires to move to a DataPage
        var destination = new DataPage(page);
        destination.AsPage().Header.PageType = DataPage.DefaultType;
        destination.Clear();

        FlushToDataPage(destination, batch, new SlottedArray(copy), children);

        ArrayPool<byte>.Shared.Return(buffer);

        RegisterForFutureReuse(children, batch);

        return destination.AsPage();
    }

    private static void RegisterForFutureReuse(ReadOnlySpan<DbAddress> children, IBatchContext batch)
    {
        foreach (var bucket in children)
        {
            if (bucket.IsNull == false)
                batch.RegisterForFutureReuse(batch.GetAt(bucket), true);
        }

        // This page is not reused as it has been repurposed to data page.
    }

    private void FlushToExisting(IBatchContext batch, in SlottedArray map)
    {
        Span<BottomPage> writableChildren = stackalloc BottomPage[Payload.BucketCount];
        var existing = 0;

        for (var i = 0; i < Payload.BucketCount; i++)
        {
            if (TryGetWritableChildAt(batch, i, out writableChildren[i]))
            {
                existing |= 1 << i;
            }
        }

        // Enumerate all and try flush
        foreach (var item in map.EnumerateAll())
        {
            if (item.Key.IsEmpty)
                continue;

            var index = GetIndex(item.Key);
            var mask = 1 << index;
            if ((existing & mask) != mask)
                continue;

            if (writableChildren[index].Map.TrySet(item.Key, item.RawData))
            {
                map.Delete(item);
            }
        }
    }

    private bool TryFlushToNew(IBatchContext batch, in SlottedArray map)
    {
        if (Data.Buckets.IsAnyNull() == false)
            return false;

        Span<ushort> stats = stackalloc ushort[SlottedArray.OneNibbleStatsCount];
        map.GatherCountStats1Nibble(stats);

        // Combine stats to be aligned with buckets count
        Span<ushort> indexed = stackalloc ushort[Payload.BucketCount];
        for (var i = 0; i < Payload.BucketCount; i++)
        {
            if (Data.Buckets[i].IsNull)
            {
                // Count only these, which are not children atm
                for (var j = 0; j < NibblesInBucket; j++)
                {
                    indexed[i] = stats[i * NibblesInBucket + j];
                }
            }
        }

        const int start = -1;

        var maxIndex = start;
        var maxValue = 0;

        for (int i = 0; i < Payload.BucketCount; i++)
        {
            if (indexed[i] > maxValue)
            {
                maxValue = indexed[i];
                maxIndex = i;
            }
        }

        if (maxIndex == start)
        {
            // None of the existing data has a shared prefix that does not exist
            return false;
        }

        Debug.Assert(Data.Buckets[maxIndex].IsNull, "Should be null");

        var child = batch.GetNewPage<BottomPage>(out var childAddr);
        Data.Buckets[maxIndex] = childAddr;
        child.Clear();

        // Enumerate all and try flush
        foreach (var item in map.EnumerateAll())
        {
            if (item.Key.IsEmpty)
                continue;

            var index = GetIndex(item.Key);
            if (index != maxIndex)
                continue;

            if (child.Map.TrySet(item.Key, item.RawData))
            {
                map.Delete(item);
            }
        }

        return true;
    }

    private static void FlushToDataPage(DataPage destination, IBatchContext batch, in SlottedArray map, ReadOnlySpan<DbAddress> children)
    {
        for (var i = 0; i < Payload.BucketCount; i++)
        {
            var bucket = children[i];

            if (bucket.IsNull)
            {
                continue;
            }

            CopyToDestination(destination, new BottomPage(batch.GetAt(bucket)).Map, batch);
        }

        // Copy all the entries from this
        CopyToDestination(destination, map, batch);
        return;

        static void CopyToDestination(DataPage destination, SlottedArray map, IBatchContext batch)
        {
            foreach (var item in map.EnumerateAll())
            {
                var result = new DataPage(destination.Set(item.Key, item.RawData, batch));

                Debug.Assert(result.AsPage().Raw == destination.AsPage().Raw, "Should not COW or replace the page");
            }
        }
    }

    private bool TryGetWritableChildAt(IBatchContext batch, int index, out BottomPage child)
    {
        var addr = Data.Buckets[index];
        if (addr.IsNull)
        {
            child = default;
            return false;
        }

        var p = batch.GetWritableCopy(batch.GetAt(addr));

        Data.Buckets[index] = batch.GetAddress(p);

        child = new BottomPage(p);
        return true;
    }

    private const int NibblesInBucket = 16 / Payload.BucketCount;

    private static int GetIndex(in NibblePath path) => path.Nibble0 % NibblesInBucket;

    public Page DeleteByPrefix(in NibblePath prefix, IBatchContext batch)
    {
        if (Header.BatchId != batch.BatchId)
        {
            // the page is from another batch, meaning, it's readonly. Copy
            var writable = batch.GetWritableCopy(page);
            return new BottomPage(writable).DeleteByPrefix(prefix, batch);
        }

        Map.DeleteByPrefix(prefix);

        return page;
    }

    public void Clear()
    {
        Map.Clear();
        Data.Buckets.Clear();
    }

    [SkipLocalsInit]
    public bool TryGet(IPageResolver batch, scoped in NibblePath key, out ReadOnlySpan<byte> result)
    {
        if (Map.TryGet(key, out result))
            return true;

        if (key.IsEmpty)
            return false;

        var index = GetIndex(key);
        var addr = Data.Buckets[index];
        if (addr.IsNull)
            return false;

        return new BottomPage(batch.GetAt(addr)).Map.TryGet(key, out result);
    }

    public static BottomPage Wrap(Page page) => Unsafe.As<Page, BottomPage>(ref page);

    public static PageType DefaultType => PageType.Bottom;
}