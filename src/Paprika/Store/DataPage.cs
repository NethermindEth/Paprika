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
    public const int ConsumedNibbles = 1;
    public const int BucketCount = DbAddressList.Of16.Count;

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

        if (prefix.Length >= ConsumedNibbles)
        {
            var index = GetBucket(prefix);
            var childAddr = buckets[index];

            if (childAddr.IsNull == false)
            {
                var sliced = prefix.SliceFrom(ConsumedNibbles);
                var child = batch.GetAt(childAddr);
                child = child.Header.PageType == PageType.DataPage
                    ? new DataPage(child).DeleteByPrefix(sliced, batch)
                    : new BottomPage(child).DeleteByPrefix(sliced, batch);
                buckets[index] = batch.GetAddress(child);
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

            Debug.Assert(k.Length >= ConsumedNibbles);

            var bucket = GetBucket(k);
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
                batch.EnsureWritableCopy(ref childAddr);
                payload.Buckets[bucket] = childAddr;
            }

            // Slice nibbles, set current, spin again
            k = k.SliceFrom(ConsumedNibbles);
            current = childAddr;
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

    private const int MerkleInMapToNibble = 2;
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

        var left = payload.MerkleLeft.IsNull
            ? batch.GetNewPage<ChildBottomPage>(out payload.MerkleLeft, header.Level)
            : new ChildBottomPage(batch.EnsureWritableCopy(ref payload.MerkleLeft));

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
    /// Whether the key belong always in the local map.
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
        private const int DataSize = Size - BucketSize - DbAddress.Size * 2;

        private const int DataOffset = Size - DataSize;

        [FieldOffset(0)] public DbAddressList.Of256 Buckets;

        [FieldOffset(BucketSize)] public DbAddress MerkleLeft;

        [FieldOffset(BucketSize + DbAddress.Size)]
        public DbAddress MerkleRight;

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
        }

        public bool IsClean => MerkleLeft.IsNull && MerkleRight.IsNull && new SlottedArray(DataSpan).IsEmpty && Buckets.IsClean;
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
                bucket = dp.Data.Buckets[GetBucket(sliced)];
                if (bucket.IsNull == false)
                    batch.Prefetch(bucket);
            }

            if (bucket.IsNull)
            {
                returnValue = false;
                break;
            }

            // non-null page jump, follow it!
            sliced = sliced.SliceFrom(ConsumedNibbles);
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
            for (int i = 0; i < BucketCount; i++)
            {
                var bucket = Data.Buckets[i];
                if (bucket.IsNull)
                {
                    continue;
                }

                var child = resolver.GetAt(bucket);
                var type = child.Header.PageType;

                builder.Push((byte)i);
                {
                    if (type == PageType.DataPage)
                    {
                        new DataPage(child).Accept(ref builder, visitor, resolver, bucket);
                    }
                    else if (type == PageType.Bottom)
                    {
                        new BottomPage(child).Accept(ref builder, visitor, resolver, bucket);
                    }
                    else if (type == PageType.ChildBottom)
                    {
                        new ChildBottomPage(child).Accept(ref builder, visitor, resolver, bucket);
                    }
                    else
                    {
                        throw new InvalidOperationException($"Invalid page type {type}");
                    }
                }
                builder.Pop();
            }
        }
    }

    public static int GetBucket(in NibblePath key) => key.Nibble0;
}