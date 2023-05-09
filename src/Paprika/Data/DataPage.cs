﻿using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Paprika.Db;

namespace Paprika.Pages;

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
public readonly unsafe struct DataPage : IDataPage
{
    private readonly Page _page;

    [DebuggerStepThrough]
    public DataPage(Page page) => _page = page;

    public ref PageHeader Header => ref _page.Header;

    public ref Payload Data => ref Unsafe.AsRef<Payload>(_page.Payload);

    public const int NibbleCount = 1;

    /// <summary>
    /// Represents the data of this data page. This type of payload stores data in 16 nibble-addressable buckets.
    /// These buckets is used to store up to <see cref="FixedMapSize"/> entries before flushing them down as other pages
    /// like page split. 
    /// </summary>
    [StructLayout(LayoutKind.Explicit, Size = Size)]
    public struct Payload
    {
        private const int Size = Page.PageSize - PageHeader.Size;

        // to align to long
        public const int BucketCount = 16;

        /// <summary>
        /// The size of the <see cref="FixedMap"/> held in this page. Must be long aligned.
        /// </summary>
        private const int FixedMapSize = Size - BucketCount * DbAddress.Size;

        private const int FixedMapOffset = Size - FixedMapSize;

        /// <summary>
        /// The first field of buckets.
        /// </summary>
        [FieldOffset(0)] private DbAddress Bucket;

        public Span<DbAddress> Buckets => MemoryMarshal.CreateSpan(ref Bucket, BucketCount);

        /// <summary>
        /// The first item of map of frames to allow ref to it.
        /// </summary>
        [FieldOffset(FixedMapOffset)] private byte FixedMapStart;

        /// <summary>
        /// Fixed map memory
        /// </summary>
        public Span<byte> FixedMapSpan => MemoryMarshal.CreateSpan(ref FixedMapStart, FixedMapSize);
    }

    /// <summary>
    /// Sets values for the given <see cref="SetContext.Path"/>
    /// </summary>
    /// <param name="ctx"></param>
    /// <param name="level">The nesting level of the call</param>
    /// <returns>
    /// The actual page which handled the set operation. Due to page being COWed, it may be a different page.
    /// 
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
        var nibble = path.FirstNibble;

        var address = Data.Buckets[nibble];

        // the bucket is not null and represents a page jump, follow it
        if (address.IsNull == false && address.IsValidPageAddress)
        {
            var page = ctx.Batch.GetAt(address);
            var updated = new DataPage(page).Set(ctx.SliceFrom(NibbleCount));

            // remember the updated
            Data.Buckets[nibble] = ctx.Batch.GetAddress(updated);
            return _page;
        }

        // try in-page write
        var map = new FixedMap(Data.FixedMapSpan);

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

        if (TryExtractAsStorageTree(biggestNibbleStats, ctx, map))
        {
            // storage cells extracted, set the value now
            return Set(ctx);
        }

        // standard nibble extraction followed by the child creation
        var child = ctx.Batch.GetNewPage(out var childAddr, true);
        var dataPage = new DataPage(child);

        foreach (var item in map.EnumerateNibble(biggestNibble))
        {
            var key = item.Key.SliceFrom(NibbleCount);
            var set = new SetContext(key, item.RawData, ctx.Batch);

            dataPage = new DataPage(dataPage.Set(set));

            // use the special delete for the item that is much faster than map.Delete(item.Key);
            map.Delete(item);
        }

        Data.Buckets[biggestNibble] = childAddr;

        // The page has some of the values flushed down, try to add again.
        return Set(ctx);
    }

    public bool TryGet(FixedMap.Key key, IReadOnlyBatchContext batch, out ReadOnlySpan<byte> result)
    {
        var nibble = key.Path.FirstNibble;
        var bucket = Data.Buckets[nibble];

        // non-null page jump, follow it!
        if (bucket.IsNull == false && bucket.IsValidPageAddress)
        {
            return new DataPage(batch.GetAt(bucket)).TryGet(key.SliceFrom(NibbleCount), batch, out result);
        }

        // read in-page
        var map = new FixedMap(Data.FixedMapSpan);

        // try first storage tree
        if (TryFindExistingStorageTreeForCellOf(map, key, out var storageTreeAddress))
        {
            var storageTree = new DataPage(batch.GetAt(storageTreeAddress));
            var inTreeAddress = FixedMap.Key.StorageTreeStorageCell(key);

            return storageTree.TryGet(inTreeAddress, batch, out result);
        }

        // try regular map
        if (map.TryGet(key, out result))
        {
            return true;
        }

        result = default;
        return false;
    }

    private static bool TryFindExistingStorageTreeForCellOf(in FixedMap map, in FixedMap.Key key,
        out DbAddress storageTreeAddress)
    {
        if (key.Type != FixedMap.DataType.StorageCell)
        {
            storageTreeAddress = default;
            return false;
        }

        var storageTreeRootKey = FixedMap.Key.StorageTreeRootPageAddress(key.Path);
        if (map.TryGet(storageTreeRootKey, out var rawPageAddress))
        {
            storageTreeAddress = DbAddress.Read(rawPageAddress);
            return true;
        }

        storageTreeAddress = default;
        return false;
    }

    private static bool TryExtractAsStorageTree((byte nibble, byte accountsCount, double percentage) biggestNibbleStats,
        in SetContext ctx, in FixedMap map)
    {
        // a prerequisite to plant a tree is a single account in the biggest nibble
        // if there are 2 or more then the accounts should be split first
        // also, create only if the nibble occupies more than 0.9 of the page
        // otherwise it's just a nibble extraction
        var shouldPlant = biggestNibbleStats.accountsCount == 1 &&
                          biggestNibbleStats.percentage > 0.9;

        if (shouldPlant == false)
            return false;

        var nibble = biggestNibbleStats.nibble;

        // required as enumerator destroys paths when enumeration moves to the next value
        Span<byte> accountPathBytes = stackalloc byte[ctx.Key.Path.MaxByteLength];

        // find account first
        foreach (var item in map.EnumerateNibble(nibble))
        {
            if (item.Type == FixedMap.DataType.Account)
            {
                accountPathBytes = item.Key.Path.WriteTo(accountPathBytes);
                break;
            }
        }

        // parse the account
        NibblePath.ReadFrom(accountPathBytes, out var accountPath);

        var storage = ctx.Batch.GetNewPage(out _, true);
        var dataPage = new DataPage(storage);

        foreach (var item in map.EnumerateNibble(nibble))
        {
            if (item.Type == FixedMap.DataType.StorageCell && item.Key.Path.Equals(accountPath))
            {
                // it's ok to use item.Key, the enumerator does not changes the additional key bytes
                var key = FixedMap.Key.StorageTreeStorageCell(item.Key);

                Serializer.ReadStorageValue(item.RawData, out var value);

                dataPage = new DataPage(dataPage.Set(new SetContext(key, item.RawData, ctx.Batch)));

                // fast delete by enumerator item
                map.Delete(item);
            }
        }

        // storage cells moved, plant the trie
        var storageTreeAddress = ctx.Batch.GetAddress(dataPage.AsPage());
        Span<byte> span = stackalloc byte[4];
        storageTreeAddress.Write(span);
        if (map.TrySet(FixedMap.Key.StorageTreeRootPageAddress(accountPath), span) == false)
        {
            throw new Exception("Critical error. Map should have been cleaned and ready to accept the write");
        }

        return true;
    }

    private static void WriteStorageCellInStorageTrie(SetContext ctx,
        DbAddress storageTreeRootPageAddress, in FixedMap map)
    {
        var storageTree = ctx.Batch.GetAt(storageTreeRootPageAddress);

        // build a new key, based just on the storage key as the root is addressed by the account address
        var inTreeAddress = FixedMap.Key.StorageTreeStorageCell(ctx.Key);

        var updatedStorageTree =
            new DataPage(storageTree).Set(new SetContext(inTreeAddress, ctx.Data, ctx.Batch));

        if (updatedStorageTree.Raw != storageTree.Raw)
        {
            // the tree was COWed, need to write back in place
            Span<byte> update = stackalloc byte[4];
            ctx.Batch.GetAddress(updatedStorageTree).Write(update);
            if (map.TrySet(FixedMap.Key.StorageTreeRootPageAddress(ctx.Key.Path), update) == false)
            {
                throw new Exception("Could not update the storage root. " +
                                    "It should always be possible as tge previous one is existing");
            }
        }
    }
}