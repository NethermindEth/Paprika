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
    public static LeafPage Wrap(Page page) => new(page);

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

        var count = Data.Buckets.LastIndexOfAnyExcept(DbAddress.Null) + 1;
        if (count == 0)
        {
            // No overflow, allocate, move all, set in page
            AllocateAndFlushToBucket(batch, ref Data.Buckets[0]);

            if (Map.TrySet(key, data) == false)
            {
                throw new Exception("Impossible. There should be space in this map");
            }

            return page;
        }

        // Some overflow pages exist, flush down to existing
        Span<LeafOverflowPage> overflows = stackalloc LeafOverflowPage[count];
        for (var i = 0; i < count; i++)
        {
            overflows[i] = new LeafOverflowPage(batch.EnsureWritableCopy(ref Data.Buckets[i]));
        }

        // Fill up existing overflow pages
        foreach (var item in Map.EnumerateAll())
        {
            var isDelete = item.RawData.Length == 0;
            if (isDelete)
            {
                foreach (var overflow in overflows)
                {
                    overflow.Map.Delete(item.Key);
                }

                Map.Delete(item);
                continue;
            }

            var set = false;
            foreach (var overflow in overflows)
            {
                if (!overflow.Map.TrySet(item.Key, item.RawData))
                {
                    continue;
                }

                set = true;
                break;
            }

            if (set)
            {
                Map.Delete(item);
            }
        }

        // After flushing down, try to flush again, if does not work
        if (Map.TrySet(key, data))
        {
            return page;
        }

        // Allocate a new bucket, and write
        if (count < BucketCount)
        {
            AllocateAndFlushToBucket(batch, ref Data.Buckets[count]);
            if (Map.TrySet(key, data))
            {
                return page;
            }
        }

        // This page is filled, move everything down. Start by registering for the reuse all the pages.
        batch.RegisterForFutureReuse(page);

        // Not enough space, transform into a data page.
        var @new = batch.GetNewPage(out _, true);

        ref var header = ref @new.Header;
        header.PageType = PageType.Standard;
        header.Level = page.Header.Level; // same level

        var dataPage = new DataPage(@new);

        foreach (var item in Map.EnumerateAll())
        {
            dataPage = new DataPage(dataPage.Set(item.Key, item.RawData, batch));
        }

        foreach (var bucket in Data.Buckets)
        {
            if (bucket.IsNull == false)
            {
                var resolved = batch.GetAt(bucket);

                batch.RegisterForFutureReuse(resolved);

                var overflow = new LeafOverflowPage(resolved);
                foreach (var item in overflow.Map.EnumerateAll())
                {
                    dataPage = new DataPage(dataPage.Set(item.Key, item.RawData, batch));
                }
            }
        }

        // Set this value and return data page
        return dataPage.Set(key, data, batch);
    }

    private void AllocateAndFlushToBucket(IBatchContext batch, ref DbAddress bucket)
    {
        Debug.Assert(bucket.IsNull);

        var overflow = AllocOverflow(batch, out bucket);
        foreach (var item in Map.EnumerateAll())
        {
            if (!overflow.Map.TrySet(item.Key, item.RawData))
            {
                // Was not able to set, break
                break;
            }

            Map.Delete(item);
        }
    }

    private LeafOverflowPage AllocOverflow(IBatchContext batch, out DbAddress addr)
    {
        var newPage = batch.GetNewPage(out addr, true);
        newPage.Header.Level = (byte)(Header.Level + 1);
        newPage.Header.PageType = PageType.LeafOverflow;
        return new LeafOverflowPage(newPage);
    }

    private const int BucketCount = 4;

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