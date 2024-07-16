using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Paprika.Data;

namespace Paprika.Store;

/// <summary>
/// Represents the lowest level of the Paprika tree. No buckets, no nothing, just data.
/// </summary>
[method: DebuggerStepThrough]
public readonly unsafe struct MerkleLeafPage(Page page) : IPageWithData<MerkleLeafPage>
{
    public static MerkleLeafPage Wrap(Page page) => Unsafe.As<Page, MerkleLeafPage>(ref page);

    private ref PageHeader Header => ref page.Header;

    private ref Payload Data => ref Unsafe.AsRef<Payload>(page.Payload);

    public Page Set(in NibblePath key, in ReadOnlySpan<byte> data, IBatchContext batch)
    {
        if (Header.BatchId != batch.BatchId)
        {
            // the page is from another batch, meaning, it's readonly. Copy
            var writable = batch.GetWritableCopy(page);
            return new MerkleLeafPage(writable).Set(key, data, batch);
        }

        if (data.IsEmpty)
        {
            Map.Delete(key);
            return page;
        }

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
        header.PageType = PageType.MerkleFanOut;
        header.Level = page.Header.Level; // same level

        var dataPage = new MerkleFanOutPage(@new);

        foreach (var item in Map.EnumerateAll())
        {
            dataPage = new MerkleFanOutPage(dataPage.Set(item.Key, item.RawData, batch));
        }

        // Set this value and return data page
        return dataPage.Set(key, data, batch);
    }

    [SkipLocalsInit]
    private bool TryFlushDownToExisting(IBatchContext batch)
    {
        var count = Data.CountOverflowPages();

        if (count == 0)
        {
            return false;
        }

        var o0 = GetOverflowWritableMap(batch, 0);

        MapSource source;
        if (count == 1)
        {
            source = new MapSource(o0);
        }
        else
        {
            var o1 = GetOverflowWritableMap(batch, 1);
            if (count == 2)
            {
                source = new MapSource(o0, o1);
            }
            else
            {
                var o2 = GetOverflowWritableMap(batch, 2);
                var o3 = GetOverflowWritableMap(batch, 3);
                if (count == 4)
                {
                    source = new MapSource(o0, o1, o2, o3);
                }
                else
                {
                    var o4 = GetOverflowWritableMap(batch, 4);
                    var o5 = GetOverflowWritableMap(batch, 5);
                    var o6 = GetOverflowWritableMap(batch, 6);
                    var o7 = GetOverflowWritableMap(batch, 7);
                    source = new MapSource(o0, o1, o2, o3, o4, o5, o6, o7);
                }
            }
        }

        Map.MoveNonEmptyKeysTo(source, true);
        return true;
    }

    private SlottedArray GetOverflowWritableMap(IBatchContext batch, int index) =>
        new LeafOverflowPage(batch.EnsureWritableCopy(ref Data.Buckets[index])).Map;

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
        private const int Size = Page.PageSize - PageHeader.Size;
        private const int DataSize = Size;

        /// <summary>
        /// The first item of map of frames to allow ref to it.
        /// </summary>
        [FieldOffset(0)] private byte DataStart;

        /// <summary>
        /// Writable area.
        /// </summary>
        public Span<byte> DataSpan => MemoryMarshal.CreateSpan(ref DataStart, DataSize);
    }

    public bool TryGet(IReadOnlyBatchContext batch, scoped in NibblePath key, out ReadOnlySpan<byte> result)
    {
        batch.AssertRead(Header);

        return Map.TryGet(key, out result);
    }

    private SlottedArray Map => new(Data.DataSpan);


    public void Report(IReporter reporter, IPageResolver resolver, int pageLevel, int trimmedNibbles)
    {
        var slotted = new SlottedArray(Data.DataSpan);
        reporter.ReportDataUsage(Header.PageType, pageLevel, trimmedNibbles, slotted);
    }

    public void Accept(IPageVisitor visitor, IPageResolver resolver, DbAddress addr)
    {
        resolver.Prefetch(Data.Buckets);

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
