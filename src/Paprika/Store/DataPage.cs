using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Paprika.Data;
using Paprika.Merkle;

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
public readonly unsafe struct DataPage(Page page) : IPage<DataPage>
{
    public static DataPage Wrap(Page page) => Unsafe.As<Page, DataPage>(ref page);
    public static PageType DefaultType => PageType.DataPage;
    public bool IsClean => Data.IsClean;

    private ref PageHeader Header => ref page.Header;

    private ref Payload Data => ref Unsafe.AsRef<Payload>(page.Payload);

    public Page DeleteByPrefix(in NibblePath prefix, IBatchContext batch)
    {
        if (Header.BatchId != batch.BatchId)
        {
            // the page is from another batch, meaning, it's readonly. Copy
            var writable = batch.GetWritableCopy(page);
            return new DataPage(writable).DeleteByPrefix(prefix, batch);
        }

        Map.DeleteByPrefix(prefix);

        ref var buckets = ref Data.Buckets;

        if (prefix.Length >= Data.ConsumedNibbles)
        {
            var index = Data.GetBucket(prefix);
            var childAddr = buckets[index];

            if (childAddr.IsNull == false)
            {
                var sliced = prefix.SliceFrom(Data.ConsumedNibbles);
                var child = batch.EnsureWritableCopy(ref childAddr);
                buckets[index] = childAddr;
                child.DeleteByPrefix(sliced, batch);
            }
        }
        else
        {
            // remove all
            for (var i = 0; i < Data.ConsumedNibbles; i++)
            {
                var childAddr = buckets[i];

                if (childAddr.IsNull)
                    continue;

                var child = batch.EnsureWritableCopy(ref childAddr);
                buckets[i] = childAddr;
                child.DeleteByPrefix(NibblePath.Empty, batch);
            }
        }

        return page;
    }

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

            if (TrySetAtBottom(k, data, batch, page))
                break;

            Debug.Assert(page.Header.PageType == PageType.DataPage);

            ref var payload = ref Unsafe.AsRef<Payload>(page.Payload);

            if (ShouldBeKeptLocal(k))
            {
                SetLocally(k, data, batch, ref payload, page.Header);
                break;
            }

            Debug.Assert(k.Length >= payload.ConsumedNibbles);

            var bucket = payload.GetBucket(k);
            var childAddr = payload.Buckets[bucket];

            if (data.IsEmpty && childAddr.IsNull)
            {
                // It's a deletion, but the child does not exist.
                break;
            }

            // Ensure child exists
            if (childAddr.IsNull)
            {
                // Create a child
                batch.GetNewPage<ChildBottomPage>(out childAddr, (byte)(page.Header.Level + 1));
                payload.Buckets[bucket] = childAddr;
            }
            else
            {
                var child = batch.EnsureWritableCopy(ref childAddr);
                payload.Buckets[bucket] = childAddr;

                if (TryTurnToFanOut(page, child, bucket, batch))
                {
                    // Spin again as the page has been turned to a fanout.
                    continue;
                }
            }

            // Slice nibbles, set current, spin again
            k = k.SliceFrom(payload.ConsumedNibbles);
            current = childAddr;
        }

        static bool TryTurnToFanOut(Page page, Page child, int bucket, IBatchContext batch)
        {
            if (page.Header.Level % 2 != 0 || page.Header.PageType != PageType.DataPage)
            {
                // Only DataPage s on even levels are transformed to fan outs.
                return false;
            }

            var @this = new DataPage(page);
            ref var payload = ref @this.Data;
            if (payload.IsFanOut)
                return false;

            // Turn to fan out only these with all children being data pages.
            if (child.Header.PageType != PageType.DataPage)
                return false;

            payload.ChildDataPages |= 1 << bucket;
            if (!payload.IsFanOut)
                return false;

            // Copy buckets to local then clear the original
            var children = payload.Buckets;
            payload.Buckets.Clear();

            for (byte i = 0; i < Payload.NotFanOutBucketCount; i++)
            {
                var p = batch.GetAt(children[i]);
                Debug.Assert(p.Header.PageType == PageType.DataPage);

                // Get the page and steal its buckets
                var dp = new DataPage(p);

                // Copy the buckets
                for (byte j = 0; j < Payload.NotFanOutBucketCount; j++)
                {
                    var index = (i << NibblePath.NibbleShift) | j;
                    payload.Buckets[index] = dp.Data.Buckets[j];

                    // The bucket is set, try to extract the single nibble key that it may store
                    if (dp.TryGet(batch, NibblePath.Single(j, 1), out var data))
                    {
                        // Construct the path that follows the ordering and allows setting the data
                        @this.Set(NibblePath.DoubleEven(i, j), data, batch);
                    }
                }

                // The child data page is no longer needed and can be recycled with its Merkle side-cars
                if (dp.Data.MerkleLeft.IsNull == false)
                {
                    batch.RegisterForFutureReuse(batch.GetAt(dp.Data.MerkleLeft), true);
                }

                if (dp.Data.MerkleRight.IsNull == false)
                {
                    batch.RegisterForFutureReuse(batch.GetAt(dp.Data.MerkleRight), true);
                }

                batch.RegisterForFutureReuse(dp.AsPage(), true);
            }

            return true;
        }
    }

    private static bool TrySetAtBottom(in NibblePath key, ReadOnlySpan<byte> data, IBatchContext batch, Page page)
    {
        var type = page.Header.PageType;
        if (type == PageType.Bottom || type == PageType.ChildBottom)
        {
            page.Set(key, data, batch);
            return true;
        }

        return false;
    }

    /// <summary>
    /// The highest single nibble path that will be stored in local map. 
    /// </summary>
    /// <remarks>
    /// Subtract 1 for the empty key and one more for additional write caching
    /// </remarks>
    private const int MerkleInMapToNibble = Page.PageSize / 600 - 1 - 1;

    private const int MerkleInRightFromInclusive = 9;

    private static void SetLocally(in NibblePath key, ReadOnlySpan<byte> data, IBatchContext batch, ref Payload payload,
        PageHeader header)
    {
        // The key should be kept locally
        var map = new SlottedArray(payload.DataSpan);

        // Check if deletion with empty local
        if (data.IsEmpty && payload.MerkleLeft.IsNull)
        {
            map.Delete(key);
            return;
        }

        // Try set locally
        if (map.TrySet(key, data))
        {
            return;
        }

        var left = batch.EnsureWritableOrGetNew<ChildBottomPage>(ref payload.MerkleLeft, header.Level);

        ChildBottomPage right;
        if (payload.MerkleRight.IsNull)
        {
            // Only left exist, try to move everything there.
            foreach (var item in map.EnumerateAll())
            {
                // We keep these three values  
                if (ShouldBeKeptLocalInMap(item.Key))
                    continue;

                if (left.Map.TrySet(item.Key, item.RawData))
                {
                    map.Delete(item);
                }
            }

            if (map.TrySet(key, data))
            {
                // All good, map can hold the data.
                return;
            }

            // Not enough space. Create the right and perform the split
            right = batch.GetNewPage<ChildBottomPage>(out payload.MerkleRight, header.Level);

            // Only left exist, try to move everything there.
            foreach (var item in left.Map.EnumerateAll())
            {
                // We keep these three values  
                if (ShouldBeKeptInRight(item.Key))
                {
                    right.Map.Set(item.Key, item.RawData);
                    left.Map.Delete(item);
                }
            }
        }

        right = new ChildBottomPage(batch.EnsureWritableCopy(ref payload.MerkleRight));

        // Redistribute keys again
        foreach (var item in map.EnumerateAll())
        {
            // We keep these three values  
            if (ShouldBeKeptLocalInMap(item.Key))
                continue;

            if (ShouldBeKeptInRight(item.Key))
            {
                if (item.RawData.IsEmpty)
                    right.Map.Delete(item.Key);
                else
                    right.Map.Set(item.Key, item.RawData);
            }
            else
            {
                if (item.RawData.IsEmpty)
                    left.Map.Delete(item.Key);
                else
                    left.Map.Set(item.Key, item.RawData);
            }

            map.Delete(item);
        }

        if (ShouldBeKeptLocalInMap(key))
        {
            map.Set(key, data);
        }
        else if (ShouldBeKeptInRight(key))
        {
            right.Map.Set(key, data);
        }
        else
        {
            left.Map.Set(key, data);
        }
    }

    private static bool ShouldBeKeptInRight(in NibblePath key) => key.Nibble0 >= MerkleInRightFromInclusive;

    private static bool ShouldBeKeptLocal(in NibblePath key) => key.IsEmpty || key.Length == 1;

    /// <summary>
    /// Whether the key belong always in the local <see cref="SlottedArray"/> over <see cref="Payload.BucketSize"/>.
    /// </summary>
    private static bool ShouldBeKeptLocalInMap(in NibblePath key) => key.IsEmpty || key.Nibble0 < MerkleInMapToNibble;

    public void Clear() => Data.Clear();

    /// <summary>
    /// Represents the data of this data page. This type of payload stores data in 16 nibble-addressable buckets.
    /// These buckets are used to store up to <see cref="DataSize"/> entries before flushing them down as other pages
    /// like page split.
    /// </summary>
    [StructLayout(LayoutKind.Explicit, Size = Size)]
    public struct Payload
    {
        public const int Size = Page.PageSize - PageHeader.Size;
        private const int BucketSize = DbAddressList.Of256.Size;

        /// <summary>
        /// The size of the raw byte data held in this page. Must be long aligned.
        /// </summary>
        private const int DataSize = Size - BucketSize - DbAddress.Size * 2 - sizeof(int);

        private const int DataOffset = Size - DataSize;


        [FieldOffset(0)] public DbAddressList.Of256 Buckets;

        [FieldOffset(BucketSize)] public DbAddress MerkleLeft;

        [FieldOffset(BucketSize + DbAddress.Size)]
        public DbAddress MerkleRight;

        [FieldOffset(BucketSize + DbAddress.Size * 2)]
        public int ChildDataPages;

        /// <summary>
        /// The first item of map of frames to allow ref to it.
        /// </summary>
        [FieldOffset(DataOffset)] private byte DataStart;

        /// <summary>
        /// Writable area.
        /// </summary>
        public Span<byte> DataSpan => MemoryMarshal.CreateSpan(ref DataStart, DataSize);

        public void Clear()
        {
            new SlottedArray(DataSpan).Clear();
            MerkleLeft = default;
            MerkleRight = default;
            Buckets.Clear();
            ChildDataPages = 0;
        }

        public bool IsClean => MerkleLeft.IsNull &&
                               MerkleRight.IsNull &&
                               ChildDataPages == 0 &&
                               new SlottedArray(DataSpan).IsEmpty &&
                               Buckets.IsClean;

        public bool IsFanOut => ChildDataPages == 0xFF_FF;
        public int ConsumedNibbles => IsFanOut ? 2 : 1;

        public const int NotFanOutBucketCount = DbAddressList.Of16.Count;

        public int BucketCount => IsFanOut ? DbAddressList.Of256.Count : NotFanOutBucketCount;

        public int GetBucket(in NibblePath key) =>
            IsFanOut ? (key.Nibble0 << NibblePath.NibbleShift) | key.GetAt(1) : key.Nibble0;
    }

    public bool TryGet(IPageResolver batch, scoped in NibblePath key, out ReadOnlySpan<byte> result)
        => TryGet(batch, key, out result, this.AsPage());

    private static bool TryGet(IPageResolver batch, scoped in NibblePath key, out ReadOnlySpan<byte> result,
        Page page)
    {
        bool returnValue;
        Unsafe.SkipInit(out result);

        var sliced = key;

        do
        {
            if (page.Header.PageType != PageType.DataPage)
            {
                // Any other, handle like this
                returnValue = page.TryGet(batch, sliced, out result);
                break;
            }

            var dp = new DataPage(page);

            if (ShouldBeKeptLocal(sliced))
            {
                returnValue = dp.TryGetLocally(sliced, batch, out result);
                break;
            }

            DbAddress bucket = default;
            if (!sliced.IsEmpty)
            {
                // As the CPU does not auto-prefetch across page boundaries
                // Prefetch child page in case we go there next to reduce CPU stalls
                bucket = dp.Data.Buckets[dp.Data.GetBucket(sliced)];
                if (bucket.IsNull == false)
                    batch.Prefetch(bucket);
            }

            if (bucket.IsNull)
            {
                returnValue = false;
                break;
            }

            // non-null page jump, follow it!
            sliced = sliced.SliceFrom(dp.Data.ConsumedNibbles);
            page = batch.GetAt(bucket);
        } while (true);

        return returnValue;
    }

    private bool TryGetLocally(scoped in NibblePath key, IPageResolver batch, out ReadOnlySpan<byte> result)
    {
        Unsafe.SkipInit(out result);

        if (Map.TryGet(key, out result))
        {
            return true;
        }

        // TODO: potential IO optimization to search left only if the right does not exist or the key does not belong to the right
        if (Data.MerkleLeft.IsNull == false && batch.GetAt(Data.MerkleLeft).TryGet(batch, key, out result))
        {
            return true;
        }

        if (Data.MerkleRight.IsNull == false && batch.GetAt(Data.MerkleRight).TryGet(batch, key, out result))
        {
            return true;
        }

        return false;
    }

    public SlottedArray Map => new(Data.DataSpan);

    public void Accept(ref NibblePath.Builder builder, IPageVisitor visitor, IPageResolver resolver, DbAddress addr)
    {
        resolver.Prefetch(Data.Buckets);

        using (visitor.On(ref builder, this, addr))
        {
            for (int i = 0; i < Data.BucketCount; i++)
            {
                var bucket = Data.Buckets[i];
                if (bucket.IsNull)
                {
                    continue;
                }

                var child = resolver.GetAt(bucket);

                if (Data.IsFanOut)
                {
                    builder.Push((byte)(i >> NibblePath.NibbleShift), (byte)(i & NibblePath.NibbleMask));
                }
                else
                {
                    builder.Push((byte)i);
                }

                child.Accept(ref builder, visitor, resolver, bucket);

                builder.Pop(Data.IsFanOut ? 2 : 1);
            }
        }
    }
}