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

        var path = ctx.Key.Path;
        var isDelete = ctx.Data.IsEmpty;
        var requiresTombstoneOnDelete = path.Length > 0;
        
        var map = new SlottedArray(Data.DataSpan);

        // if written value is a storage cell, try to find the storage tree first
        if (TryFindExistingStorageTreeForCellOf(map, ctx.Key, out var storageTreeAddress))
        {
            // tree exists, write in it
            WriteStorageCellInStorageTrie(ctx, storageTreeAddress, map);
            return _page;
        }

        // try write in map
        if (map.TrySet(ctx.Key, ctx.Data))
        {
            return _page;
        }

        // the map is full, extraction must follow
        var biggestNibbleStats = map.GetBiggestNibbleStats();
        var biggestNibble = biggestNibbleStats.nibble;

        // first test should that be a storage tree
        if (TryExtractAsStorageTree(biggestNibbleStats, ctx, map))
        {
            // storage cells extracted, set the value now
            return Set(ctx);
        }

        // try get the child page
        ref var address = ref Data.Buckets[biggestNibble];
        Page child;
        
        if (address.IsNull)
        {
            // there is no child page, create one
            child = ctx.Batch.GetNewPage(out Data.Buckets[biggestNibble], true);
            child.Header.TreeLevel = (byte)(Header.TreeLevel + 1);
            child.Header.PageType = Header.PageType;
        }
        else
        {
            // the child page is not-null, retrieve it
            child = ctx.Batch.GetNewPage(out address, true);
        }

        var dataPage = new DataPage(child);

        foreach (var item in map.EnumerateNibble(biggestNibble))
        {
            var key = item.Key.SliceFrom(NibbleCount);
            var set = new SetContext(key, item.RawData, ctx.Batch);

            dataPage = new DataPage(dataPage.Set(set));

            // use the special delete for the item that is much faster than map.Delete(item.Key);
            map.Delete(item);
        }

        address = ctx.Batch.GetAddress(dataPage.AsPage());

        // The page has some of the values flushed down, try to add again.
        return Set(ctx);
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

        public const int BucketCount = 16;

        /// <summary>
        /// The size of the raw byte data held in this page. Must be long aligned.
        /// </summary>
        private const int DataSize = Size - BucketCount * DbAddress.Size;

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
        // read in-page
        var map = new SlottedArray(Data.DataSpan);

        // try first storage tree
        if (TryFindExistingStorageTreeForCellOf(map, key, out var storageTreeAddress))
        {
            var storageTree = new DataPage(batch.GetAt(storageTreeAddress));
            var inTreeAddress = Key.StorageTreeStorageCell(key);

            return storageTree.TryGet(inTreeAddress, batch, out result);
        }

        // try regular map
        if (map.TryGet(key, out result))
        {
            return true;
        }
    
        // path longer than 0, try to find in child
        if (key.Path.Length > 0)
        {
            // try to go deeper only if the path is long enough
            var bucket = Data.Buckets[key.Path.FirstNibble];

            // non-null page jump, follow it!
            if (bucket.IsNull == false)
            {
                return new DataPage(batch.GetAt(bucket)).TryGet(key.SliceFrom(NibbleCount), batch, out result);
            }
        }

        result = default;
        return false;
    }

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

        reporter.ReportDataUsage(level,
            Payload.BucketCount - emptyBuckets, new SlottedArray(Data.DataSpan).Count);
    }

    private static bool TryFindExistingStorageTreeForCellOf(in SlottedArray map, in Key key,
        out DbAddress storageTreeAddress)
    {
        if (key.Type != DataType.StorageCell)
        {
            storageTreeAddress = default;
            return false;
        }

        var storageTreeRootKey = Key.StorageTreeRootPageAddress(key.Path);
        if (map.TryGet(storageTreeRootKey, out var rawPageAddress))
        {
            storageTreeAddress = DbAddress.Read(rawPageAddress);
            return true;
        }

        storageTreeAddress = default;
        return false;
    }

    private static bool TryExtractAsStorageTree((byte nibble, double storageCellPercentageInPage) biggestNibbleStats,
        in SetContext ctx, in SlottedArray map)
    {
        // A prerequisite to plan a massive storage tree is to have at least 90% of the page occupied by a single nibble
        // storage cells. If then they share the same key, we're ready to extract
        var hasEnoughOfStorageCells = biggestNibbleStats.storageCellPercentageInPage > 0.9;

        if (hasEnoughOfStorageCells == false)
            return false;

        var nibble = biggestNibbleStats.nibble;

        // required as enumerator destroys paths when enumeration moves to the next value
        Span<byte> accountPathBytes = stackalloc byte[ctx.Key.Path.MaxByteLength];
        NibblePath accountPath = default;

        // assert that all StorageCells have the same prefix
        foreach (var item in map.EnumerateNibble(nibble))
        {
            if (item.Type == DataType.StorageCell)
            {
                if (accountPath.Equals(NibblePath.Empty))
                {
                    NibblePath.ReadFrom(item.Key.Path.WriteTo(accountPathBytes), out accountPath);
                }
                else
                {
                    if (item.Key.Path.Equals(accountPath) == false)
                    {
                        // If there's at least one item that has a different key, it won't be a massive storage tree.
                        return false;
                    }
                }
            }
        }

        var storage = ctx.Batch.GetNewPage(out _, true);

        // this is the top page of the massive storage tree
        storage.Header.TreeLevel = 0;
        storage.Header.PageType = PageType.MassiveStorageTree;

        var dataPage = new DataPage(storage);

        foreach (var item in map.EnumerateNibble(nibble))
        {
            // no need to check whether the path is the same because it was checked above.
            if (item.Type == DataType.StorageCell)
            {
                // it's ok to use item.Key, the enumerator does not changes the additional key bytes
                var key = Key.StorageTreeStorageCell(item.Key);

                dataPage = new DataPage(dataPage.Set(new SetContext(key, item.RawData,
                    ctx.Batch)));

                // fast delete by enumerator item
                map.Delete(item);
            }
        }

        // storage cells moved, plant the trie
        var storageTreeAddress = ctx.Batch.GetAddress(dataPage.AsPage());
        Span<byte> span = stackalloc byte[4];
        storageTreeAddress.Write(span);
        if (map.TrySet(Key.StorageTreeRootPageAddress(accountPath), span) == false)
        {
            throw new Exception("Critical error. Map should have been cleaned and ready to accept the write");
        }

        return true;
    }

    private static void WriteStorageCellInStorageTrie(SetContext ctx,
        DbAddress storageTreeRootPageAddress, in SlottedArray map)
    {
        var storageTree = ctx.Batch.GetAt(storageTreeRootPageAddress);

        // build a new key, based just on the storage key as the root is addressed by the account address
        var inTreeAddress = Key.StorageTreeStorageCell(ctx.Key);

        var updatedStorageTree =
            new DataPage(storageTree).Set(new SetContext(inTreeAddress, ctx.Data, ctx.Batch));

        if (updatedStorageTree.Raw != storageTree.Raw)
        {
            // the tree was COWed, need to write back in place
            Span<byte> update = stackalloc byte[4];
            ctx.Batch.GetAddress(updatedStorageTree).Write(update);
            if (map.TrySet(Key.StorageTreeRootPageAddress(ctx.Key.Path), update) == false)
            {
                throw new Exception("Could not update the storage root. " +
                                    "It should always be possible as tge previous one is existing");
            }
        }
    }
}