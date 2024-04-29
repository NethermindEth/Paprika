using System.Buffers.Binary;
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

        // Try set in-map first
        if (Map.TrySet(key, data))
        {
            return page;
        }

        // The map is full, try flush to the existing buckets
        var count = Data.Buckets.LastIndexOfAnyExcept(DbAddress.Null) + 1;

        if (count == 0)
        {
            // No overflow, create one
            var overflow = AllocOverflow(batch, out Data.Buckets[0]);
            Map.MoveTo(overflow.Map);

            if (Map.TrySet(key, data))
            {
                return page;
            }

            Debug.Fail("Should have moved to the first overflow successfully");
        }

        // Flush down to existing
        TryFlushDownToExisting(batch, count);

        // After flushing down, try to set again
        if (Map.TrySet(key, data))
        {
            return page;
        }

        if (count < BucketCount)
        {
            return DoubleSizeAndSet(key, data, batch);
        }

        // This page is filled, move everything down. Start by registering for the reuse all the pages.
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

    private Page DoubleSizeAndSet(in NibblePath key, in ReadOnlySpan<byte> data, IBatchContext batch)
    {
        var count = Data.Buckets.LastIndexOfAnyExcept(DbAddress.Null) + 1;
        Debug.Assert(count < BucketCount);

        // Create an empty copy
        var copy = new LeafPage(batch.GetNewPage(out _, true));
        copy.Header.Level = Header.Level;
        copy.Header.PageType = Header.PageType;

        // Double the size, allocate overflows in the copy
        count *= 2;
        for (var i = 0; i < count; i++)
        {
            AllocOverflow(batch, out copy.Data.Buckets[i]);
        }

        // Reuse existing overflows and move to the copy
        var existing = Data.Buckets[..(count / 2)]; // the previous
        foreach (var overflow in existing)
        {
            var p = batch.GetAt(overflow);

            Debug.Assert(p.Header.PageType == PageType.LeafOverflow);

            batch.RegisterForFutureReuse(p);

            foreach (var item in new LeafOverflowPage(p).Map.EnumerateAll())
            {
                copy = new LeafPage(copy.Set(item.Key, item.RawData, batch));
            }
        }

        foreach (var item in Map.EnumerateAll())
        {
            copy = new LeafPage(copy.Set(item.Key, item.RawData, batch));
        }

        return copy.Set(key, data, batch);
    }

    private void TryFlushDownToExisting(IBatchContext batch, int count)
    {
        foreach (var item in Map.EnumerateAll())
        {
            if (item.Key.IsEmpty)
            {
                continue;
            }

            var slot = item.Key.FirstNibble % count;
            var overflow = batch.EnsureWritableCopy(ref Data.Buckets[slot]);

            var isDelete = item.RawData.IsEmpty;
            if (isDelete)
            {
                new LeafOverflowPage(overflow).Map.Delete(item.Key);
                Map.Delete(item);
            }
            else if (new LeafOverflowPage(overflow).Map.TrySet(item.Key, item.RawData))
            {
                Map.Delete(item);
            }
        }
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

        // Try set in-situ and return cowed page
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