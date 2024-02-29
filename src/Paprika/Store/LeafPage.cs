﻿using System.Buffers.Binary;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Paprika.Data;

namespace Paprika.Store;

/// <summary>
/// Represents the lowest level of the Paprika tree. No buckets, no nothing, just data.
/// </summary>
[method: DebuggerStepThrough]
public readonly unsafe struct LeafPage(Page page) : IPageWithData<LeafPage>
{
    private static readonly byte[] IdPlaceHolder = new byte[2];

    public static LeafPage Wrap(Page page) => new(page);

    public bool IsNull => page.Raw == UIntPtr.Zero;

    private ref PageHeader Header => ref page.Header;

    private ref Payload Data => ref Unsafe.AsRef<Payload>(page.Payload);

    public Page Set(in NibblePath key, in ReadOnlySpan<byte> data, IBatchContext batch)
    {
        if (Header.BatchId != batch.BatchId)
        {
            // the page is from another batch, meaning, it's readonly. Copy
            var writable = batch.GetWritableCopy(page);
            return new LeafPage(writable).Set(key, data, batch);
        }

        // Check if value exists, if it does, delete it
        if (Map.TryGet(key, out var result))
        {
            // The key exists so overflow must as well
            var (bucket, id) = Decode(result);
            var overflow = new LeafOverflowPage(batch.GetAt(Data.Buckets[bucket]));

            Data.Buckets[bucket] = batch.GetAddress(overflow.Delete(id, batch));
            Map.Delete(key);
        }

        var isDelete = data.IsEmpty;
        if (isDelete)
        {
            // If the operation is a delete, there's nothing more to do as removal was done above
            return page;
        }

        if (Map.CapacityLeft >= SlottedArray.EstimateNeededCapacity(key, IdPlaceHolder))
        {
            ref var bucket = ref Data.Buckets[0];

            // has capacity to write the id, search for the bucket
            Page p;
            if (bucket.IsNull)
            {
                p = batch.GetNewPage(out bucket, true);
                p.Header.Level = (byte)(Header.Level + 1);
                p.Header.PageType = PageType.LeafOverflow;
            }
            else
            {
                p = batch.GetAt(bucket);
            }

            var overflow = new LeafOverflowPage(p);
            if (overflow.CanStore(data))
            {
                var (cowed, id) = overflow.Add(data, batch);

                var encoded = Encode(0, id, stackalloc byte[2]);
                if (!Map.TrySet(key, encoded))
                {
                    throw new Exception("Should have space to put id in after the check above");
                }

                bucket = batch.GetAddress(cowed);
                return page;
            }
        }

        // This page is filled, move everything down. Start by registering for the reuse all the pages.
        batch.RegisterForFutureReuse(page);
        foreach (var b in Data.Buckets)
        {
            if (b.IsNull == false)
            {
                batch.RegisterForFutureReuse(batch.GetAt(b));
            }
        }

        // Not enough space, transform into a data page.
        var @new = batch.GetNewPage(out _, true);

        ref var header = ref @new.Header;
        header.PageType = PageType.Standard;
        header.Level = page.Header.Level; // same level

        var dataPage = new DataPage(@new);

        foreach (var item in Map.EnumerateAll())
        {
            var (bucket, id) = Decode(item.RawData);
            var copyFrom = new LeafOverflowPage(batch.GetAt(Data.Buckets[bucket]));

            if (copyFrom.TryGet(id, out var toCopy) == false)
            {
                throw new Exception("Failed to find the value");
            }

            dataPage = new DataPage(dataPage.Set(item.Key, toCopy, batch));
        }

        // Set this value and return data page
        return dataPage.Set(key, data, batch);
    }

    private const int BucketCount = 8;
    private const int BucketMask = 7;
    private const int BucketShift = 3;


    private static (byte bucket, ushort id) Decode(in ReadOnlySpan<byte> data)
    {
        Debug.Assert(data.Length == 2);

        var value = BinaryPrimitives.ReadUInt16LittleEndian(data);

        return ((byte)(value & BucketMask), (ushort)(value >> BucketShift));
    }

    private static ReadOnlySpan<byte> Encode(byte bucket, ushort id, Span<byte> destination)
    {
        BinaryPrimitives.WriteUInt16LittleEndian(destination, (ushort)((id << BucketShift) | bucket));
        return destination[..2];
    }

    public (Page page, bool) TrySet(in NibblePath key, in ReadOnlySpan<byte> data, IBatchContext batch)
    {
        var map = Map;

        // Check whether should set in this leaf, make this as simple and as fast as possible starting from simple checks:
        // 1. if data is empty, it's a delete
        // 2. if there's a capacity in the map, just write it
        // 3. if the data is an update and can be put in the map

        var shouldTrySet =
            data.IsEmpty ||
            SlottedArray.EstimateNeededCapacity(key, IdPlaceHolder) <= map.CapacityLeft ||
            map.HasSpaceToUpdateExisting(key, IdPlaceHolder);

        if (shouldTrySet == false)
        {
            return (page, false);
        }

        if (Header.BatchId != batch.BatchId)
        {
            // The page is from another batch, meaning, it's readonly. COW
            // It could be useful to check whether the map will accept the write first, before doing COW,
            // but this would result in a check for each TrySet. This should be implemented in map. 
            var writable = batch.GetWritableCopy(page);
            return (new LeafPage(writable).Set(key, data, batch), true);
        }

        return (Set(key, data, batch), true);
    }

    [StructLayout(LayoutKind.Explicit, Size = Size)]
    private struct Payload
    {
        private const int BucketSize = DbAddress.Size * BucketCount;
        private const int Size = Page.PageSize - PageHeader.Size;
        private const int DataSize = Size - BucketSize;

        [FieldOffset(0)] private DbAddress BucketStart;
        public Span<DbAddress> Buckets => MemoryMarshal.CreateSpan(ref BucketStart, BucketCount);

        /// <summary>
        /// The first item of map of frames to allow ref to it.
        /// </summary>
        [FieldOffset(BucketSize)] private byte DataStart;

        /// <summary>
        /// Writable area.
        /// </summary>
        public Span<byte> DataSpan => MemoryMarshal.CreateSpan(ref DataStart, DataSize);
    }

    public bool TryGet(scoped NibblePath key, IReadOnlyBatchContext batch, out ReadOnlySpan<byte> result)
    {
        batch.AssertRead(Header);

        if (Map.TryGet(key, out var data) == false)
        {
            result = default;
            return false;
        }

        var (bucket, id) = Decode(data);
        var overflow = new LeafOverflowPage(batch.GetAt(Data.Buckets[bucket]));
        return overflow.TryGet(id, out result);
    }

    private SlottedArray Map => new(Data.DataSpan);

    public int CapacityLeft => Map.CapacityLeft;

    public void Report(IReporter reporter, IPageResolver resolver, int level)
    {
        var slotted = new SlottedArray(Data.DataSpan);
        reporter.ReportDataUsage(Header.PageType, level, 0, slotted.Count, slotted.CapacityLeft);

        foreach (var bucket in Data.Buckets)
        {
            if (bucket.IsNull == false)
            {
                new LeafOverflowPage(resolver.GetAt(bucket)).Report(reporter, resolver, level + 1);
            }
        }
    }

    public void Accept(IPageVisitor visitor, IPageResolver resolver, DbAddress addr)
    {
        using var scope = visitor.On(this, addr);

        foreach (var bucket in Data.Buckets)
        {
            if (bucket.IsNull == false)
            {
                new LeafOverflowPage(resolver.GetAt(bucket)).Accept(visitor, resolver, bucket);
            }
        }
    }
}