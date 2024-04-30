using System.Buffers.Binary;
using System.Diagnostics;
using System.Numerics;
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
    public static LeafPage Wrap(Page page) => Unsafe.As<Page, LeafPage>(ref page);

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

        var (success, cow) = TrySet(key, data, batch);
        if (success)
            return cow;

        // No place in the existing map, no place in the overflows, time to grow.
        TryAllocateOverflowsAndFlushDown(batch);

        // Try set in-map first
        if (Map.TrySet(key, data))
        {
            return page;
        }

        // It was not possible to set the value in the page. 
        // This page is filled, move everything down and create a DataPage in this place
        batch.RegisterForFutureReuse(page);

        // Not enough space, transform into a data page.
        var @new = batch.GetNewPage(out _, true);

        ref var header = ref @new.Header;
        header.PageType = PageType.Standard;
        header.Level = page.Header.Level; // same level

        var dataPage = new DataPage(@new);

        foreach (var bucket in Data.Buckets)
        {
            if (bucket.IsNull)
                continue;

            var resolved = batch.GetAt(bucket);

            batch.RegisterForFutureReuse(resolved);

            var overflow = new LeafOverflowPage(resolved);
            foreach (var item in overflow.Map.EnumerateAll())
            {
                dataPage = new DataPage(dataPage.Set(item.Key, item.RawData, batch));
            }
        }

        foreach (var item in Map.EnumerateAll())
        {
            dataPage = new DataPage(dataPage.Set(item.Key, item.RawData, batch));
        }

        // Set this value and return data page
        return dataPage.Set(key, data, batch);
    }

    private bool TryAllocateOverflowsAndFlushDown(IBatchContext batch)
    {
        var count = Data.CountOverflowPages();
        if (count == BucketCount)
        {
            return false;
        }

        if (count == 0)
        {
            var overflow = AllocOverflow(batch, out Data.Buckets[0]);
            Map.MoveNonEmptyKeysTo(overflow.Map);
            return true;
        }

        // We don't COW this Leaf. It is much harder to reason and implement.
        // What we do is that we allocate overflows first, then redistribute and flush down.
        // This has the same behavior as a COW but requires no juggling with the page.

        Span<DbAddress> existing = stackalloc DbAddress[count];
        Data.Buckets[..count].CopyTo(existing);

        // Double the size, allocate overflows in the copy
        var newCount = count * 2;
        Span<LeafOverflowPage> overflows = stackalloc LeafOverflowPage[newCount];

        for (var i = 0; i < newCount; i++)
        {
            overflows[i] = AllocOverflow(batch, out Data.Buckets[i]);
        }

        // Redistribute the overflows
        foreach (var overflow in existing)
        {
            var p = batch.GetAt(overflow);

            Debug.Assert(p.Header.PageType == PageType.LeafOverflow);

            batch.RegisterForFutureReuse(p);

            foreach (var item in new LeafOverflowPage(p).Map.EnumerateAll())
            {
                Debug.Assert(item.Key.IsEmpty == false, "The key in overflow cannot be empty!");

                var at = item.Key.FirstNibble % newCount;
                if (overflows[at].Map.TrySet(item.Key, item.RawData) == false)
                {
                    Debug.Fail("Overflow should be able to copy to overflow");
                }
            }
        }

        TryFlushDownToExisting(batch);

        return true;
    }

    private bool TryFlushDownToExisting(IBatchContext batch)
    {
        var count = Data.CountOverflowPages();

        if (count == 0)
        {
            return false;
        }

        foreach (var item in Map.EnumerateAll())
        {
            if (item.Key.IsEmpty)
            {
                continue;
            }

            var slot = item.Key.FirstNibble % count;
            var overflow = batch.EnsureWritableCopy(ref Data.Buckets[slot]);
            var map = new LeafOverflowPage(overflow).Map;

            var isDelete = item.RawData.IsEmpty;
            if (isDelete)
            {
                map.Delete(item.Key);
                Map.Delete(item);
            }
            else if (map.TrySet(item.Key, item.RawData))
            {
                Map.Delete(item);
            }
        }

        return true;
    }

    private LeafOverflowPage AllocOverflow(IBatchContext batch, out DbAddress addr)
    {
        var newPage = batch.GetNewPage(out addr, true);
        newPage.Header.Level = (byte)(Header.Level + 1);
        newPage.Header.PageType = PageType.LeafOverflow;
        return new LeafOverflowPage(newPage);
    }

    private const int BucketCount = 8;

    [StructLayout(LayoutKind.Explicit, Size = Size)]
    private struct Payload
    {
        private const int BucketSize = DbAddress.Size * BucketCount;
        private const int Size = Page.PageSize - PageHeader.Size;
        private const int DataSize = Size - BucketSize;

        [FieldOffset(0)] private DbAddress BucketStart;
        public Span<DbAddress> Buckets => MemoryMarshal.CreateSpan(ref BucketStart, BucketCount);

        public int CountOverflowPages() => Buckets.LastIndexOfAnyExcept(DbAddress.Null) + 1;

        /// <summary>
        /// The first item of map of frames to allow ref to it.
        /// </summary>
        [FieldOffset(BucketSize)] private byte DataStart;

        /// <summary>
        /// Writable area.
        /// </summary>
        public Span<byte> DataSpan => MemoryMarshal.CreateSpan(ref DataStart, DataSize);
    }

    public (bool success, Page cow) TrySet(in NibblePath key, ReadOnlySpan<byte> data, IBatchContext batch)
    {
        if (Header.BatchId != batch.BatchId)
        {
            // the page is from another batch, meaning, it's readonly. Copy
            var writable = batch.GetWritableCopy(page);
            return new LeafPage(writable).TrySet(key, data, batch);
        }

        // Try set in-map first
        if (Map.TrySet(key, data))
        {
            return (true, page);
        }

        // The map is full, try flush to the existing buckets, then retry
        if (TryFlushDownToExisting(batch) == false)
        {
            return (false, page);
        }

        return (Map.TrySet(key, data), page);
    }

    public bool TryGet(IReadOnlyBatchContext batch, scoped in NibblePath key, out ReadOnlySpan<byte> result)
    {
        batch.AssertRead(Header);

        if (Map.TryGet(key, out result))
        {
            return true;
        }

        foreach (var bucket in Data.Buckets)
        {
            if (bucket.IsNull == false)
            {
                if (new LeafOverflowPage(batch.GetAt(bucket)).Map.TryGet(key, out result))
                {
                    return true;
                }
            }
        }

        return false;
    }

    private SlottedArray Map => new(Data.DataSpan);

    public int CapacityLeft => Map.CapacityLeft;

    public void Report(IReporter reporter, IPageResolver resolver, int pageLevel, int trimmedNibbles)
    {
        var slotted = new SlottedArray(Data.DataSpan);
        reporter.ReportDataUsage(Header.PageType, pageLevel, trimmedNibbles, slotted);

        foreach (var bucket in Data.Buckets)
        {
            if (bucket.IsNull == false)
            {
                new LeafOverflowPage(resolver.GetAt(bucket)).Report(reporter, resolver, pageLevel + 1, trimmedNibbles);
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