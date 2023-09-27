using System.Buffers;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Paprika.Crypto;
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
    public const int BucketCount = 16;

    private readonly Page _page;

    [DebuggerStepThrough]
    public DataPage(Page page) => _page = page;

    public ref PageHeader Header => ref _page.Header;

    public ref Payload Data => ref Unsafe.AsRef<Payload>(_page.Payload);

    public const int NibbleCount = 1;

    /// <summary>
    /// Sets values for the given <see cref="SetContext.Key"/>
    /// </summary>
    /// <returns>
    /// The actual page which handled the set operation. Due to page being COWed, it may be a different page.
    /// </returns>
    public Page Set(in SetContext ctx)
    {
        if (Header.BatchId != ctx.Batch.BatchId)
        {
            // the page is from another batch, meaning, it's readonly. Copy
            var writable = ctx.Batch.GetWritableCopy(_page);

            return new DataPage(writable).Set(ctx);
        }

        var map = new SlottedArray(Data.DataSpan);
        var key = TryCompress(ctx.Key);

        var path = key.Path;
        var isDelete = ctx.Data.IsEmpty;

        if (isDelete)
        {
            if (path.Length < NibbleCount)
            {
                // path cannot be held on a lower level so delete in here
                return DeleteInMap(key, map);
            }

            // path is not empty, so it might have a child page underneath with data, let's try
            var childPageAddress = Data.Buckets[path.FirstNibble];
            if (childPageAddress.IsNull)
            {
                // there's no lower level, delete in map
                return DeleteInMap(key, map);
            }
        }

        // try write in map
        if (map.TrySet(key, ctx.Data))
        {
            return _page;
        }

        // The map is full. The plan for this is the following:
        // 1. try apply the dictionary compression
        // 2. try to flush down softly
        // 3. try to flush down with force the biggest nibble
        if (TryCompressMap(map))
        {
            // compression occured, try to write again
            if (map.TrySet(key, ctx.Data))
            {
                return _page;
            }
        }

        if (TryFlushDownSoftAndWrite(key, ctx.Data, ctx.Batch, map))
            return _page;

        // Flushing down to existing pages didn't make space for this entry, flush the biggest nibble forcefully.
        var biggestNibble = FindMostFrequentNibble(map);

        // try get the child page
        ref var address = ref Data.Buckets[biggestNibble];
        Page child;

        if (address.IsNull)
        {
            // create child as the same type as the parent
            child = ctx.Batch.GetNewPage(out Data.Buckets[biggestNibble], true);
            child.Header.PageType = Header.PageType;
            child.Header.Level = (byte)(Header.Level + 1);
        }
        else
        {
            // the child page is not-null, retrieve it
            child = ctx.Batch.GetAt(address);
        }

        var dataPage = new DataPage(child);
        var batch = ctx.Batch;

        // flush down: force
        dataPage = FlushDown(map, biggestNibble, dataPage, batch, false);
        address = ctx.Batch.GetAddress(dataPage.AsPage());

        // The page has some of the values flushed down, try to add again.
        return Set(ctx);
    }

    private bool TryCompressMap(in SlottedArray map)
    {
        if (Header.Level < DictionaryCompression.CompressFromLevel)
            return false;

        if (!Data.Compression.HasMoreSpace)
            return false;

        var dictionary = new Dictionary<int, int>();

        foreach (var item in map.EnumerateAll())
        {
            if (IsCompressible(item.Key))
            {
                var hashCode = item.Key.Path.GetHashCode();
                ref var count = ref CollectionsMarshal.GetValueRefOrAddDefault(dictionary, hashCode, out _);
                count++;
            }
        }

        if (dictionary.Count == 0)
            return false;

        // find max and compress it away
        var max = dictionary.Max(kvp => kvp.Value);
        var maxKey = dictionary.First(kvp => kvp.Value == max).Key;

        // assigned compressed
        foreach (var item in Map.EnumerateAll())
        {
            if (IsCompressible(item.Key))
            {
                var hashCode = item.Key.Path.GetHashCode();
                if (hashCode == maxKey)
                {
                    Data.Compression.Assign(item.Key.Path);
                    break;
                }
            }
        }

        // copy over
        var size = Data.DataSpan.Length;
        var array = ArrayPool<byte>.Shared.Rent(size);
        var bytes = array.AsSpan(0, size);
        bytes.Clear();

        var copy = new SlottedArray(bytes);
        foreach (var item in map.EnumerateAll())
        {
            copy.TrySet(TryCompress(item.Key), item.RawData);
        }

        bytes.CopyTo(Data.DataSpan);
        ArrayPool<byte>.Shared.Return(array);

        return true;
    }

    private bool TryFlushDownSoftAndWrite(in Key key, in ReadOnlySpan<byte> data, IBatchContext batch, in SlottedArray map)
    {
        // There's no more room in this page. We need to make some, but in a way that will not over-allocate pages.
        // To make it work:
        // 1. find amongst existing children pages that have some capacity left.
        // 2. sort them from the most empty to the least empty
        // 3. loop through them, and flush down the given nibble in a way that will not allocate anything in them
        // 4. after each spin of the loop, try to write map

        // TODO: Change this algorithm into a simple bit-map that is memoized in page.
        // Softly flushed map would memoize whether a page was flushed softly.
        // Here, select only these that were not

        Span<ushort> capacities = stackalloc ushort[BucketCount];
        Span<byte> nibbles = stackalloc byte[BucketCount];

        for (byte i = 0; i < BucketCount; i++)
        {
            nibbles[i] = i;

            var childAddress = Data.Buckets[i];
            if (childAddress.IsNull == false)
            {
                capacities[i] = (ushort)new DataPage(batch.GetAt(childAddress)).Map.CapacityLeft;
            }
        }

        capacities.Sort(nibbles);
        var start = capacities.IndexOfAnyExcept((ushort)0);

        if (start == -1)
            return false;

        // contains sorted from the smallest to the biggest capacity
        var nibblesWithSomeCapacity = nibbles.Slice(start);
        for (var i = nibblesWithSomeCapacity.Length - 1; i >= 0; i--)
        {
            var nibble = nibblesWithSomeCapacity[i];
            ref var childAddress = ref Data.Buckets[nibble];
            Debug.Assert(childAddress.IsNull == false, "Only an existing child should be selected for flush down");

            var page = new DataPage(batch.GetAt(childAddress));
            page = FlushDown(map, nibble, page, batch, true);

            // update the child address
            childAddress = batch.GetAddress(page.AsPage());

            // try write again to the map
            if (map.TrySet(key, data))
            {
                return true;
            }
        }

        return false;
    }

    private static DataPage FlushDown(in SlottedArray map, byte nibble, DataPage destination, IBatchContext batch, bool tryAllocFree)
    {
        const int minimumCapacity = 128;

        foreach (var item in map.EnumerateNibble(nibble))
        {
            var key = item.Key.SliceFrom(NibbleCount);

            if (tryAllocFree && destination.Map.CapacityLeft < minimumCapacity)
            {
                // if there's not that much space left in the child, break this loop
                break;
            }

            var set = new SetContext(key, item.RawData, batch);
            destination = new DataPage(destination.Set(set));

            // use the special delete for the item that is much faster than map.Delete(item.Key);
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

    private Page DeleteInMap(in Key key, SlottedArray map)
    {
        map.Delete(key);
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
        private const int DataSize = Size - BucketSize - DictionaryCompression.Size;

        private const int DataOffset = Size - DataSize;

        /// <summary>
        /// The first field of buckets.
        /// </summary>
        [FieldOffset(0)] private DbAddress Bucket;

        public Span<DbAddress> Buckets => MemoryMarshal.CreateSpan(ref Bucket, BucketCount);

        [FieldOffset(BucketSize)] public DictionaryCompression Compression;

        /// <summary>
        /// The first item of map of frames to allow ref to it.
        /// </summary>
        [FieldOffset(DataOffset)] private byte DataStart;

        /// <summary>
        /// Writable area.
        /// </summary>
        public Span<byte> DataSpan => MemoryMarshal.CreateSpan(ref DataStart, DataSize);
    }

    /// <summary>
    /// This structure provides an in-page dictionary compression,
    /// allowing to store to up to <see cref="MaxPrefixCount"/> nibbles paths extracted.
    /// </summary>
    [StructLayout(LayoutKind.Explicit, Size = Size)]
    public struct DictionaryCompression
    {
        public const int CompressFromLevel = 3;
        public const int MinimalPathLength = 2;

        public const int Size = MaxPrefixCount * AccountSize + sizeof(long);

        private const int AccountSize = Keccak.Size;
        private const int MaxPrefixCount = 2;

        [FieldOffset(0)]
        private byte pathStart;

        private Span<byte> this[byte index] =>
            MemoryMarshal.CreateSpan(ref Unsafe.Add(ref pathStart, index * AccountSize), AccountSize);

        [FieldOffset(MaxPrefixCount * AccountSize)]
        private byte Count;

        [FieldOffset(MaxPrefixCount * AccountSize + 1)]
        private byte NibbleStart;

        private Span<byte> NibblePairs => MemoryMarshal.CreateSpan(ref NibbleStart, AccountSize);

        public bool HasMoreSpace => Count < MaxPrefixCount;

        public byte Assign(in NibblePath path)
        {
            if (!HasMoreSpace)
                throw new Exception("Filled compression");

            var id = Count;
            Count++;
            path.WriteTo(this[id]);
            NibblePairs[id] = GetTwoNibbles(path);

            return id;
        }

        private static byte GetTwoNibbles(in NibblePath path) =>
            (byte)((path.GetAt(0) << NibblePath.NibbleShift) | path.GetAt(1));

        public bool TryFindCompressed(in NibblePath path, out byte id)
        {
            var search = GetTwoNibbles(path);

            for (byte i = 0; i < Count; i++)
            {
                if (NibblePairs[i] == search)
                {
                    NibblePath.ReadFrom(this[i], out var compressed);
                    if (compressed.Equals(path))
                    {
                        id = i;
                        return true;
                    }
                }
            }

            id = default;
            return false;
        }
    }


    public bool TryGet(Key key, IReadOnlyBatchContext batch, out ReadOnlySpan<byte> result) =>
        TryGet(key, (IPageResolver)batch, out result);

    private bool TryGet(Key key, IPageResolver batch, out ReadOnlySpan<byte> result)
    {
        scoped var k = TryCompress(key);

        // read in-page
        var map = Map;

        // try regular map
        if (map.TryGet(k, out result))
        {
            return true;
        }

        // path longer than 0, try to find in child
        if (k.Path.Length > 0)
        {
            // try to go deeper only if the path is long enough
            var bucket = Data.Buckets[k.Path.FirstNibble];

            // non-null page jump, follow it!
            if (bucket.IsNull == false)
            {
                return new DataPage(batch.GetAt(bucket)).TryGet(k.SliceFrom(NibbleCount), batch, out result);
            }
        }

        result = default;
        return false;
    }

    private Key TryCompress(in Key key)
    {
        if (Header.Level < DictionaryCompression.CompressFromLevel)
            return key;

        if (!IsCompressible(key))
            return key;

        if (!Data.Compression.TryFindCompressed(key.Path, out var id))
            return key;

        Debug.Assert(Header.Level <= 32);
        var actual = (byte)(Header.Level << 2 | id);

        return Key.Raw(key.StoragePath, key.Type | DataType.Compressed, NibblePath.OfByte(actual));
    }

    private static bool IsCompressible(in Key key)
    {
        // compress only merkle for storage or storage
        return key.Path.Length > DictionaryCompression.MinimalPathLength
               && (key.Type & DataType.Compressed) != DataType.Compressed
               && key.StoragePath.Length > 0;
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
            reporter.ReportItem(item.Key, item.RawData);
        }

        reporter.ReportDataUsage(Header.PageType, level, BucketCount - emptyBuckets, slotted.Count, slotted.CapacityLeft);
    }
}