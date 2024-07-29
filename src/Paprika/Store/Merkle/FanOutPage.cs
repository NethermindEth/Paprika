using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Paprika.Data;
using Paprika.Merkle;

namespace Paprika.Store.Merkle;

/// <summary>
/// A Merkle fan-out page keeps two levels of Merkle data in the overflow pages
/// leaving the data area to write-through cache for lower layers.
/// </summary>
/// <param name="page"></param>
[method: DebuggerStepThrough]
public readonly unsafe struct FanOutPage(Page page) : IPageWithData<FanOutPage>
{
    public static FanOutPage Wrap(Page page) => Unsafe.As<Page, FanOutPage>(ref page);

    private const int ConsumedNibbles = ComputeMerkleBehavior.SkipRlpMemoizationForTopLevelsCount;
    private const int BucketCount = 16 * 16;

    private static int GetIndex(scoped in NibblePath key) => (key.GetAt(0) << NibblePath.NibbleShift) + key.GetAt(1);

    private ref PageHeader Header => ref page.Header;

    private ref Payload Data => ref Unsafe.AsRef<Payload>(page.Payload);

    public Page Set(in NibblePath key, in ReadOnlySpan<byte> data, IBatchContext batch)
    {
        if (Header.BatchId != batch.BatchId)
        {
            // the page is from another batch, meaning, it's readonly. Copy
            var writable = batch.GetWritableCopy(page);
            return new FanOutPage(writable).Set(key, data, batch);
        }

        if (Data.MerkleNodes.TrySet(key, data, batch))
        {
            return page;
        }

        var isDelete = data.IsEmpty;

        Debug.Assert(key.Length >= ConsumedNibbles, "Key is meant for the next level");

        var childIndex = GetIndex(key);
        ref var childAddr = ref Data.Buckets[childIndex];
        var slotted = new SlottedArray(Data.DataSpan);

        // If it's a delete and child does not exist, delete in-situ
        if (isDelete && childAddr.IsNull)
        {
            slotted.Delete(key);
            return page;
        }

        // Write through optimization for cases
        // where the child exist and was written during this batch
        if (childAddr.IsNull == false && batch.WasWritten(childAddr))
        {
            slotted.Delete(key);
            SetInChild(ref childAddr, key.SliceFrom(ConsumedNibbles), data, batch);
            return page;
        }

        // Try set in page in write-through cache
        if (slotted.TrySet(key, data))
            return page;

        // Map is filled, flush down items, first to existent children
        if (TryFlushDownToExisting(slotted, batch))
        {
            // Try set again
            if (slotted.TrySet(key, data))
                return page;
        }

        // Try create new till flushed down
        do
        {
            FlushDownToTheBiggestNewChild(slotted, batch);
        } while (slotted.TrySet(key, data) == false);

        return page;
    }

    private bool TryFlushDownToExisting(in SlottedArray slotted, IBatchContext batch)
    {
        var anyFlushes = false;

        foreach (var item in slotted.EnumerateAll())
        {
            var index = GetIndex(item.Key);
            ref var addr = ref Data.Buckets[index];

            if (addr.IsNull)
                continue;

            var sliced = item.Key.SliceFrom(ConsumedNibbles);
            SetInChild(ref addr, sliced, item.RawData, batch);
            slotted.Delete(item);
            anyFlushes = true;
        }

        return anyFlushes;
    }

    private void FlushDownToTheBiggestNewChild(in SlottedArray slotted, IBatchContext batch)
    {
        var biggest = FindMostFrequentIndex(slotted);

        ref var child = ref Data.Buckets[biggest];

        Debug.Assert(child.IsNull, "Only non existing should be scanned after TryFlush above");

        // Gather stats first
        foreach (var item in slotted.EnumerateAll())
        {
            var index = GetIndex(item.Key);
            if (index != biggest)
            {
                continue;
            }


            var sliced = item.Key.SliceFrom(ConsumedNibbles);
            SetInChild(ref child, sliced, item.RawData, batch);
            slotted.Delete(item);
        }
    }

    private static int FindMostFrequentIndex(in SlottedArray map)
    {
        // Gather stats first
        const int count = 256;
        Span<ushort> stats = stackalloc ushort[count];

        foreach (var item in map.EnumerateAll())
        {
            stats[GetIndex(item.Key)]++;
        }

        var biggestIndex = 0;
        for (int i = 1; i < count; i++)
        {
            if (stats[i] > stats[biggestIndex])
            {
                biggestIndex = i;
            }
        }

        return biggestIndex;
    }

    private void SetInChild(ref DbAddress addr, in NibblePath key, in ReadOnlySpan<byte> data, IBatchContext batch)
    {
        if (addr.IsNull)
        {
            var child = batch.GetNewPage(out addr, true);

            child.Header.Level = (byte)(Header.Level + ConsumedNibbles);
            child.Header.PageType = PageType.MerkleLeaf;
            new LeafPage(child).Set(key, data, batch);

            return;
        }

        var existing = batch.GetAt(addr);
        var updated =
            existing.Header.PageType == PageType.MerkleLeaf
                ? new LeafPage(existing).Set(key, data, batch)
                : new FanOutPage(existing).Set(key, data, batch);

        addr = batch.GetAddress(updated);
    }

    public bool TryGet(IReadOnlyBatchContext batch, scoped in NibblePath key, out ReadOnlySpan<byte> result)
    {
        if (Data.MerkleNodes.TryGet(key, out result, batch))
            return true;

        // Try search write-through data
        if (new SlottedArray(Data.DataSpan).TryGet(key, out result))
        {
            return true;
        }

        // Failed, follow path
        var index = GetIndex(key);
        var addr = Data.Buckets[index];
        if (addr.IsNull)
        {
            result = default;
            return false;
        }

        var child = batch.GetAt(addr);
        var sliced = key.SliceFrom(ConsumedNibbles);

        return child.Header.PageType == PageType.MerkleLeaf
            ? new LeafPage(child).TryGet(batch, sliced, out result)
            : new FanOutPage(child).TryGet(batch, sliced, out result);
    }

    /// <summary>
    /// Represents the data of this data page. This type of payload stores data in 16 nibble-addressable buckets.
    /// These buckets are used to store up to <see cref="DataSize"/> entries before flushing them down as other pages
    /// like page split.
    /// </summary>
    [StructLayout(LayoutKind.Explicit, Size = Size)]
    private struct Payload
    {
        private const int Size = Page.PageSize - PageHeader.Size;
        private const int BucketSize = BucketCount * DbAddress.Size;

        /// <summary>
        /// The size of the raw byte data held in this page. Must be long aligned.
        /// </summary>
        private const int DataSize = Size - BucketSize - MerkleNodes.Size;

        private const int DataOffset = Size - DataSize;

        /// <summary>
        /// The first field of buckets.
        /// </summary>
        [FieldOffset(0)] private DbAddress Bucket;

        public Span<DbAddress> Buckets => MemoryMarshal.CreateSpan(ref Bucket, BucketCount);

        [FieldOffset(BucketSize)] public MerkleNodes MerkleNodes;

        /// <summary>
        /// The first item of map of frames to allow ref to it.
        /// </summary>
        [FieldOffset(DataOffset)] private byte DataStart;

        /// <summary>
        /// Writable area.
        /// </summary>
        public Span<byte> DataSpan => MemoryMarshal.CreateSpan(ref DataStart, DataSize);
    }

    public void Report(IReporter reporter, IPageResolver resolver, int pageLevel, int trimmedNibbles)
    {
        foreach (var bucket in Data.Buckets)
        {
            if (!bucket.IsNull)
            {
                var consumedNibbles = trimmedNibbles + ConsumedNibbles;
                var lvl = pageLevel + 1;

                var child = resolver.GetAt(bucket);

                if (child.Header.PageType == PageType.MerkleLeaf)
                    new LeafPage(child).Report(reporter, resolver, lvl, consumedNibbles);
                else
                    new FanOutPage(child).Report(reporter, resolver, lvl, consumedNibbles);
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
                if (child.Header.PageType == PageType.MerkleLeaf)
                    new LeafPage(child).Accept(visitor, resolver, bucket);
                else
                    new FanOutPage(child).Accept(visitor, resolver, bucket);
            }
        }
    }
}