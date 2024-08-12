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
public readonly unsafe struct DataPage(Page page) : IPageWithData<DataPage>, IClearable
{
    private const int ConsumedNibbles = 1;
    private const int BucketCount = DbAddressList.Of16.Count;

    private static class Modes
    {
        /// <summary>
        /// <see cref="Payload.Buckets"/> are used as regular fan out navigation.
        /// </summary>
        public const byte Fanout = 0;

        /// <summary>
        /// <see cref="Payload.Buckets"/> are used to keep the structure of the Merkle using <see cref="UShortPage"/>.
        /// Buckets[0] is used to store <see cref="NibblePath"/> of lengths of 0 and 1.
        /// Buckets[1] is used to store <see cref="NibblePath"/> of lengths of 2.
        /// Buckets[2] is used to store <see cref="NibblePath"/> of lengths of 3.
        /// Up to <see cref="DataPage.MerkleModeLimitExclusive"/>.
        /// </summary>
        public const byte Merkle = 1;
    }

    public static DataPage Wrap(Page page) => Unsafe.As<Page, DataPage>(ref page);

    private ref PageHeader Header => ref page.Header;

    private ref Payload Data => ref Unsafe.AsRef<Payload>(page.Payload);

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

            if (page.Header.Metadata == Modes.Fanout)
            {
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

                // Get new page without clearing. Clearing is done manually.
                var child = batch.GetNewPage(out childAddr, false);
                new DataPage(child).Clear();

                child.Header.PageType = PageType.Standard;
                child.Header.Level = (byte)(page.Header.Level + ConsumedNibbles);

                // Set the mode for the new child to Merkle to make it spread content on the NibblePath length basis
                child.Header.Metadata = Modes.Merkle;
                payload.Buckets[nibble] = childAddr;

                FlushDown(map, nibble, childAddr, batch);
                // Spin again to try to set.
            }
            else
            {
                Debug.Assert(page.Header.Metadata == Modes.Merkle);

                if (k.Length < MerkleMode.MerkleModeLimitExclusive)
                {
                    if (MerkleMode.TrySetInMerkle(k, data, ref payload, batch))
                    {
                        // Update happened, return
                        break;
                    }

                    // Failed to set, transform and continue.
                    MerkleMode.TransformMerkleToFanOut(current, ref page.Header, ref payload, batch);
                    continue;
                }

                // The key is not qualified to be stored under Merkle, try set in map.
                if (map.TrySet(k, data))
                {
                    // Update happened, return
                    break;
                }

                // Failed to set, transform and continue.
                MerkleMode.TransformMerkleToFanOut(current, ref page.Header, ref payload, batch);
                continue;
            }
        }
    }

    [SkipLocalsInit]
    private static class MerkleMode
    {
        // For now, use only levels with length of 0 and 1
        public const int MerkleModeLimitExclusive = 2;

        private const ushort OddMarker = 0x8000;
        private const ushort OddMarkerShift = 15;
        private const ushort Length1Marker = 0x4000;
        private const ushort Length1NibbleMask = 0x00FF;

        private static (ushort id, byte index) MapToId(in NibblePath key)
        {
            Debug.Assert(key.Length < MerkleModeLimitExclusive,
                "The key length should have been checked before calling this");

            var odd = (ushort)(key.Oddity * OddMarker);

            return key.Length switch
            {
                0 => (id: 0, index: 0),
                1 => (id: (ushort)(Length1Marker + key.FirstNibble + odd), index: 0),
                //2 => (id: (ushort)(key.FirstNibble * 16 + key.GetAt(1) + odd), index: 1),
                _ => default((ushort id, byte index))
            };
        }

        public static bool TrySetInMerkle(in NibblePath key, in ReadOnlySpan<byte> data, ref Payload payload,
            IBatchContext batch)
        {
            var (id, index) = MapToId(key);

            var addr = payload.Buckets[index];
            UShortPage child;

            if (addr.IsNull)
            {
                var page = batch.GetNewPage(out addr, false);
                payload.Buckets[index] = addr;

                page.Header.PageType = PageType.UShort;
                child = new UShortPage(page);
                child.Clear();
            }
            else
            {
                child = new UShortPage(batch.EnsureWritableCopy(ref addr));
                payload.Buckets[index] = addr;
            }

            if (data.IsEmpty)
            {
                child.Map.Delete(id);
                return true;
            }

            return child.Map.TrySet(id, data);
        }

        public static void TransformMerkleToFanOut(DbAddress at, ref PageHeader header, ref Payload payload,
            IBatchContext batch)
        {
            Debug.Assert(header.Metadata == Modes.Merkle);

            // The main thing is to notice that either fanout or merkle, they still set data in the slotted array.
            // What we can do is to:
            // 1. change the mode
            // 2. iterate over Merkle parts and set them in the page as is
            // The only thing to do is to copy them first and clean the buckets

            header.Metadata = Modes.Fanout;

            // Length 0 & 1
            var addr = payload.Buckets[0];

            if (addr.IsNull == false)
            {
                payload.Buckets[0] = DbAddress.Null;
                var page0And1 = new UShortPage(batch.GetAt(addr));
                batch.RegisterForFutureReuse(page0And1.AsPage());

                foreach (var item in page0And1.Map.EnumerateAll())
                {
                    var key = NibblePath.Empty;

                    if ((item.Key & Length1Marker) == Length1Marker)
                    {
                        var odd = (item.Key & OddMarker) >> OddMarkerShift;
                        var nibble = (byte)(item.Key & Length1NibbleMask);

                        key = NibblePath.Single(nibble, odd);
                    }

                    Set(at, key, item.RawData, batch);
                }
            }
        }

        public static bool TryGetInMerkle(IReadOnlyBatchContext batch, scoped in NibblePath key, in Payload payload, out ReadOnlySpan<byte> result)
        {
            var (id, index) = MapToId(key);
            var addr = payload.Buckets[index];

            if (addr.IsNull)
            {
                result = default;
                return false;
            }

            return new UShortPage(batch.GetAt(addr)).Map.TryGet(id, out result);
        }
    }

    public void Clear()
    {
        new SlottedArray(Data.DataSpan).Clear();
        Data.Buckets.Clear();
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

        [FieldOffset(0)] public DbAddressList.Of16 Buckets;

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

    private static bool TryGet(IReadOnlyBatchContext batch, scoped in NibblePath key, out ReadOnlySpan<byte> result,
        DataPage page)
    {
        var returnValue = false;
        var sliced = key;

        do
        {
            batch.AssertRead(page.Header);

            if (page.Header.Metadata == Modes.Merkle)
            {
                if (sliced.Length < MerkleMode.MerkleModeLimitExclusive)
                {
                    return MerkleMode.TryGetInMerkle(batch, sliced, page.Data, out result);
                }

                return page.Map.TryGet(sliced, out result);
            }

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
            if (bucket.IsNull)
                continue;

            var child = resolver.GetAt(bucket);


            if (IsFanOut)
            {
                new DataPage(child).Report(reporter, resolver, pageLevel + 1, trimmedNibbles + 1);
            }
            else
            {
                // TODO UshortPage
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

                if (IsFanOut)
                {
                    new DataPage(child).Accept(visitor, resolver, bucket);
                }
                else
                {
                    new UShortPage(child).Accept(visitor, resolver, bucket);
                }
            }
        }
    }

    private bool IsFanOut => Header.Metadata == Modes.Fanout;
}