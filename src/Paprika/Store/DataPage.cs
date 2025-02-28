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
    public const int ConsumedNibbles = 2;
    public const int BucketCount = DbAddressList.Of256.Count;

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
                child = child.Header.PageType == PageType.DataPage ?
                    new DataPage(child).DeleteByPrefix(sliced, batch) :
                    new BottomPage(child).DeleteByPrefix(sliced, batch);
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

            if (page.Header.PageType == PageType.Bottom)
            {
                var result = new BottomPage(page).Set(k, data, batch);

                Debug.Assert(result.Equals(page), "The page should have been copied before");

                return;
            }

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
                batch.GetNewPage<BottomPage>(out childAddr, (byte)(page.Header.Level + 1));
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

    private static bool ShouldBeKeptLocal(in NibblePath key) => key.Length < ConsumedNibbles;

    private static void SetLocally(in NibblePath key, ReadOnlySpan<byte> data, IBatchContext batch, ref Payload payload,
        PageHeader header)
    {
        // The key should be kept locally
        var map = new SlottedArray(payload.DataSpan);

        // Check if deletion with empty local
        if (data.IsEmpty && payload.Local.IsNull)
        {
            map.Delete(key);
            return;
        }

        // Try set locally
        if (map.TrySet(key, data))
        {
            return;
        }

        var local = payload.Local.IsNull
            ? batch.GetNewPage<BottomPage>(out payload.Local, header.Level)
            : new BottomPage(batch.EnsureWritableCopy(ref payload.Local));

        // Move all
        foreach (var item in map.EnumerateAll())
        {
            local.Set(item.Key, item.RawData, batch);
        }
        map.Clear();

        map.Set(key, data);
    }

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
        private const int DataSize = Size - BucketSize - DbAddress.Size;

        private const int DataOffset = Size - DataSize;

        [FieldOffset(0)] public DbAddressList.Of256 Buckets;

        [FieldOffset(BucketSize)]
        public DbAddress Local;

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
            Local = default;
            Buckets.Clear();
        }

        public bool IsClean => Local.IsNull && new SlottedArray(DataSpan).IsEmpty && Buckets.IsClean;
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
            if (page.Header.PageType == PageType.Bottom)
            {
                return new BottomPage(page).TryGet(batch, sliced, out result);
            }

            var dp = new DataPage(page);

            if (ShouldBeKeptLocal(sliced))
            {
                if (dp.Map.TryGet(sliced, out result))
                {
                    returnValue = true;
                    break;
                }

                returnValue = !dp.Data.Local.IsNull && new BottomPage(batch.GetAt(dp.Data.Local)).TryGet(batch, sliced, out result);
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

                var (nibble0, nibble1) = GetBucketNibbles(i);

                builder.Push(nibble0, nibble1);
                {
                    if (type == PageType.DataPage)
                    {
                        new DataPage(child).Accept(ref builder, visitor, resolver, bucket);
                    }
                    else if (type == PageType.Bottom)
                    {
                        new BottomPage(child).Accept(ref builder, visitor, resolver, bucket);
                    }
                    else
                    {
                        throw new InvalidOperationException($"Invalid page type {type}");
                    }
                }
                builder.Pop(2);
            }
        }
    }

    public static int GetBucket(in NibblePath key) =>
        (key.Nibble0 << NibblePath.NibbleShift) | (key.Length == 1 ? 0 : key.GetAt(1));

    public static (byte nibble0, byte nibble1) GetBucketNibbles(int bucket) => (
        (byte)(bucket >> NibblePath.NibbleShift), (byte)(bucket & NibblePath.NibbleMask));
}