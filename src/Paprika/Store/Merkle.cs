﻿using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Paprika.Data;
using Paprika.Merkle;

namespace Paprika.Store;

/// <summary>
/// Special pages aligned with Merkle.
/// </summary>
public static class Merkle
{
    [method: DebuggerStepThrough]
    public readonly unsafe struct StateRootPage(Page page) : IPageWithData<StateRootPage>
    {
        public static StateRootPage Wrap(Page page) => Unsafe.As<Page, StateRootPage>(ref page);

        private const int ConsumedNibbles = ComputeMerkleBehavior.SkipRlpMemoizationForTopLevelsCount;
        private const int BucketCount = 16 * 16;

        private static int GetIndex(scoped in NibblePath key) =>
            (key.GetAt(0) << NibblePath.NibbleShift) + key.GetAt(1);

        private ref PageHeader Header => ref page.Header;

        private ref Payload Data => ref Unsafe.AsRef<Payload>(page.Payload);

        public Page Set(in NibblePath key, in ReadOnlySpan<byte> data, IBatchContext batch)
        {
            if (Header.BatchId != batch.BatchId)
            {
                // the page is from another batch, meaning, it's readonly. Copy
                var writable = batch.GetWritableCopy(page);
                return new StateRootPage(writable).Set(key, data, batch);
            }

            if (key.Length < ConsumedNibbles)
            {
                var map = new SlottedArray(Data.DataSpan);
                var isDelete = data.IsEmpty;
                if (isDelete)
                {
                    map.Delete(key);
                }
                else
                {
                    var succeeded = map.TrySet(key, data);
                    Debug.Assert(succeeded);
                }

                return page;
            }

            var index = GetIndex(key);
            var sliced = key.SliceFrom(ConsumedNibbles);
            ref var addr = ref Data.Buckets[index];

            if (addr.IsNull)
            {
                var child = batch.GetNewPage(out addr, true);
                child.Header.Level = 1;
                child.Header.PageType = PageType.Standard;
                new DataPage(child).Set(sliced, data, batch);
            }
            else
            {
                addr = batch.GetAddress(new DataPage(batch.GetAt(addr)).Set(sliced, data, batch));
            }

            return page;
        }

        /// <summary>
        /// Represents the data of this data page. This type of payload stores data in 16 nibble-addressable buckets.
        /// These buckets is used to store up to <see cref="DataSize"/> entries before flushing them down as other pages
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
            private const int DataSize = Size - BucketSize;

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

        public bool TryGet(IReadOnlyBatchContext batch, scoped in NibblePath key, out ReadOnlySpan<byte> result)
        {
            if (key.Length < ConsumedNibbles)
            {
                return new SlottedArray(Data.DataSpan).TryGet(key, out result);
            }

            var index = GetIndex(key);
            var addr = Data.Buckets[index];
            if (addr.IsNull)
            {
                result = default;
                return false;
            }

            return new DataPage(batch.GetAt(addr)).TryGet(batch, key.SliceFrom(ConsumedNibbles), out result);
        }

        private SlottedArray Map => new(Data.DataSpan);


        public void Report(IReporter reporter, IPageResolver resolver, int pageLevel, int trimmedNibbles)
        {
            foreach (var bucket in Data.Buckets)
            {
                if (!bucket.IsNull)
                {
                    var consumedNibbles = trimmedNibbles + ConsumedNibbles;
                    var lvl = pageLevel + 1;

                    var child = resolver.GetAt(bucket);

                    if (child.Header.PageType == PageType.Leaf)
                        new LeafPage(child).Report(reporter, resolver, lvl, consumedNibbles);
                    else
                        new DataPage(child).Report(reporter, resolver, lvl, consumedNibbles);
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
                    if (child.Header.PageType == PageType.Leaf)
                        new LeafPage(child).Accept(visitor, resolver, bucket);
                    else
                        new DataPage(child).Accept(visitor, resolver, bucket);
                }
            }
        }
    }
}

/// <summary>
/// A Merkle fan-out page keeps two levels of Merkle data in the overflow pages
/// leaving the data area to write-through cache for lower layers.
/// </summary>
/// <param name="page"></param>
[method: DebuggerStepThrough]
public readonly unsafe struct MerkleFanOutPage(Page page) : IPageWithData<MerkleFanOutPage>
{
    public static MerkleFanOutPage Wrap(Page page) => Unsafe.As<Page, MerkleFanOutPage>(ref page);

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
            return new MerkleFanOutPage(writable).Set(key, data, batch);
        }

        var isDelete = data.IsEmpty;
        
        ref var local = ref TryFindLocal(key, out var id);
        if (Unsafe.IsNullRef(ref local) == false)
        {
            var p = new UShortPage(batch.EnsureWritableExists(ref local));
            var map = p.Map;

            if (isDelete)
            {
                map.Delete(id);
            }
            else
            {
                map.Set(id, data);
            }

            return page;
        }

        Debug.Assert(key.Length >= ConsumedNibbles, "Key is meant for the next level");

        // If child exists and was written this batch, just pass through
        var childIndex = GetIndex(key);
        ref var childAddr = ref Data.Buckets[childIndex];

        if (childAddr.IsNull == false && batch.WasWritten(childAddr))
        {
            SetInChild(ref childAddr, key.SliceFrom(ConsumedNibbles), data, batch);
            return page;
        }
        
        // Try set in page in write-through cache
        var slotted = new SlottedArray(Data.DataSpan);
        if (slotted.TrySet(key, data))
            return page;

        // Map is filled, flush down items, first to existent children
        FlushDown(key, data, batch, slotted, true);
        
        // Try set again
        if (slotted.TrySet(key, data))
            return page;
        
        // This means that there are child pages that should be flushed but were not
        FlushDown(key, data, batch, slotted, false);
        
        // Try set again
        slotted.Set(key, data);
        return page;
    }

    private void FlushDown(in NibblePath key, ReadOnlySpan<byte> data, IBatchContext batch, in SlottedArray slotted, bool toExistingOnly)
    {
        foreach (var item in slotted.EnumerateAll())
        {
            var index = GetIndex(item.Key);
            ref var addr = ref Data.Buckets[index];

            if (toExistingOnly && addr.IsNull) 
                continue;
            
            SetInChild(ref addr, key.SliceFrom(ConsumedNibbles), data, batch);
            slotted.Delete(item);
        }
    }

    private void SetInChild(ref DbAddress addr, in NibblePath key, in ReadOnlySpan<byte> data, IBatchContext batch)
    {
        if (addr.IsNull)
        {
            var child = batch.GetNewPage(out addr, true);
            
            child.Header.Level = (byte)(Header.Level + ConsumedNibbles);
            child.Header.PageType = PageType.Leaf;
            new LeafPage(child).Set(key, data, batch);
            
            return;
        }

        var existing = batch.GetAt(addr);
        var updated =
            existing.Header.PageType == PageType.Leaf
                ? new LeafPage(existing).Set(key, data, batch)
                : new MerkleFanOutPage(existing).Set(key, data, batch);

        addr = batch.GetAddress(updated);
    }

    private ref DbAddress TryFindLocal(in NibblePath key, out ushort id)
    {
        if (key.Length >= ConsumedNibbles)
        {
            id = default;
            return ref Unsafe.NullRef<DbAddress>();
        }

        id = (ushort)(key.IsEmpty ? 0 : key.FirstNibble + 1);
        const int merklePerPage = 6;
        return ref Data.LocalMerkleNodes[id / merklePerPage];
    }

    /// <summary>
    /// Represents the data of this data page. This type of payload stores data in 16 nibble-addressable buckets.
    /// These buckets is used to store up to <see cref="DataSize"/> entries before flushing them down as other pages
    /// like page split.
    /// </summary>
    [StructLayout(LayoutKind.Explicit, Size = Size)]
    private struct Payload
    {
        private const int Size = Page.PageSize - PageHeader.Size;
        private const int BucketSize = BucketCount * DbAddress.Size;
        private const int LocalMerkleNodeCount = 3;
        private const int LocalMerkleNodeSize = DbAddress.Size * LocalMerkleNodeCount;

        /// <summary>
        /// The size of the raw byte data held in this page. Must be long aligned.
        /// </summary>
        private const int DataSize = Size - BucketSize - LocalMerkleNodeSize;

        private const int DataOffset = Size - DataSize;

        /// <summary>
        /// The first field of buckets.
        /// </summary>
        [FieldOffset(0)] private DbAddress Bucket;

        public Span<DbAddress> Buckets => MemoryMarshal.CreateSpan(ref Bucket, BucketCount);

        [FieldOffset(BucketSize)] private DbAddress LocalMerkleNode;
        public Span<DbAddress> LocalMerkleNodes => MemoryMarshal.CreateSpan(ref LocalMerkleNode, LocalMerkleNodeCount);

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
    {
        ref var local = ref TryFindLocal(key, out var id);
        if (Unsafe.IsNullRef(ref local) == false)
        {
            var p = new UShortPage(batch.GetAt(local));
            return p.Map.TryGet(id, out result);
        }

        // Try search write-through
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
    
        return child.Header.PageType == PageType.Leaf
            ? new LeafPage(child).TryGet(batch, sliced, out result)
            : new MerkleFanOutPage(child).TryGet(batch, sliced, out result);
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

                if (child.Header.PageType == PageType.Leaf)
                    new LeafPage(child).Report(reporter, resolver, lvl, consumedNibbles);
                else
                    new DataPage(child).Report(reporter, resolver, lvl, consumedNibbles);
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
                if (child.Header.PageType == PageType.Leaf)
                    new LeafPage(child).Accept(visitor, resolver, bucket);
                else
                    new DataPage(child).Accept(visitor, resolver, bucket);
            }
        }
    }
}