using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Paprika.Data;

namespace Paprika.Store;

/// <summary>
/// Represents the leaf page that can be split in two.
/// </summary>
[method: DebuggerStepThrough]
public readonly unsafe struct LeafPage(Page page) : IPageWithData<LeafPage>
{
    public static LeafPage Wrap(Page page) => new(page);

    private ref PageHeader Header => ref page.Header;

    private ref Payload Data => ref Unsafe.AsRef<Payload>(page.Payload);

    private bool UseThis(in NibblePath key) =>
        key.IsEmpty || Data.Next.IsNull || key.GetAt(Header.LevelOddity) % 2 == 0;

    private (bool, Page) TrySetInSitu(in NibblePath key, in ReadOnlySpan<byte> data, IBatchContext batch)
    {
        if (Header.BatchId != batch.BatchId)
        {
            // the page is from another batch, meaning, it's readonly. Copy
            var writable = batch.GetWritableCopy(page);
            return new LeafPage(writable).TrySetInSitu(key, data, batch);
        }

        if (key.IsEmpty)
        {
            Map.Delete(key.RawSpan);
            return (true, page);
        }

        // Optimize so that the capacity is assessed before writing.
        var result = Map.TrySet(key.RawSpan, data);
        return (result, page);
    }

    public Page Set(in NibblePath key, in ReadOnlySpan<byte> data, IBatchContext batch)
    {
        if (Header.BatchId != batch.BatchId)
        {
            // the page is from another batch, meaning, it's readonly. Copy
            var writable = batch.GetWritableCopy(page);
            return new LeafPage(writable).Set(key, data, batch);
        }

        var useThis = UseThis(key);

        if (useThis)
        {
            var (success, cowed) = TrySetInSitu(key, data, batch);
            if (success)
            {
                return cowed;
            }

            // Not enough space, see whether can partition
            if (Data.Next.IsNull)
            {
                // Create next and split
                var next = new LeafPage(batch.GetNewPage(out Data.Next, true));
                next.Header.PageType = PageType.Leaf;
                next.Header.Level = Header.Level;

                foreach (var item in Map.EnumerateAll())
                {
                    // Next is already assigned so it's save to check with UseThis
                    var path = NibblePath.FromKey(item.Key).SliceFrom(Header.LevelOddity);
                    if (UseThis(path) == false)
                    {
                        var (nextSuccess, nextCowed) = next.TrySetInSitu(path, item.RawData, batch);

                        Debug.Assert(nextCowed.Raw == next.AsPage().Raw, "Inserting above should happen in the same batch");

                        if (nextSuccess)
                        {
                            Map.Delete(item);
                        }
                    }
                }

                // Retry adding after moving half to next
                return Set(key, data, batch);
            }

            // Next exist, but it's this page that is filled. Create a data
            return TransformToDataPage(key, data, batch);
        }
        else
        {
            Debug.Assert(Data.Next.IsNull == false, "The next should exist!");

            var next = new LeafPage(batch.GetAt(Data.Next));
            var (success, cowed) = next.TrySetInSitu(key, data, batch);
            if (success)
            {
                Data.Next = batch.GetAddress(cowed);
                return page;
            }

            // No success, transform to the data page
            return TransformToDataPage(key, data, batch);
        }
    }

    private Page TransformToDataPage(in NibblePath key, in ReadOnlySpan<byte> data, IBatchContext batch)
    {
        batch.RegisterForFutureReuse(this.AsPage());

        var dataPage = new DataPage(batch.GetNewPage(out _, true));
        dataPage.Header.PageType = PageType.Standard;
        dataPage.Header.Level = Header.Level;

        // Flush all of this page
        foreach (var item in Map.EnumerateAll())
        {
            var path = NibblePath.FromKey(item.Key).SliceFrom(Header.LevelOddity);
            dataPage = new DataPage(dataPage.Set(path, item.RawData, batch));
        }

        // Flush all of the next, if exist
        if (Data.Next.IsNull == false)
        {
            var next = new LeafPage(batch.GetAt(Data.Next));

            foreach (var item in next.Map.EnumerateAll())
            {
                var path = NibblePath.FromKey(item.Key).SliceFrom(Header.LevelOddity);
                dataPage = new DataPage(dataPage.Set(path, item.RawData, batch));
            }

            batch.RegisterForFutureReuse(next.AsPage());
        }

        // add the new
        return dataPage.Set(key, data, batch);
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

        /// <summary>
        /// The size of the raw byte data held in this page. Must be long aligned.
        /// </summary>
        private const int DataSize = Size - DataOffset;

        private const int DataOffset = DbAddress.Size;

        /// <summary>
        /// The address to store store keys with odd first byte.
        /// </summary>
        [FieldOffset(0)] public DbAddress Next;

        /// <summary>
        /// The first item of map of frames to allow ref to it.
        /// </summary>
        [FieldOffset(DataOffset)] private byte DataStart;

        /// <summary>
        /// Writable area.
        /// </summary>
        public Span<byte> DataSpan => MemoryMarshal.CreateSpan(ref DataStart, DataSize);
    }

    public bool TryGet(scoped NibblePath key, IReadOnlyBatchContext batch, out ReadOnlySpan<byte> result)
    {
        batch.AssertRead(Header);

        var useThis = UseThis(key);

        var raw = key.RawSpan;

        return useThis
            ? Map.TryGet(raw, out result)
            : new LeafPage(batch.GetAt(Data.Next)).Map.TryGet(raw, out result);
    }

    private SlottedArray Map => new(Data.DataSpan);

    public void Report(IReporter reporter, IPageResolver resolver, int level)
    {
        var slotted = new SlottedArray(Data.DataSpan);

        reporter.ReportDataUsage(Header.PageType, level, Data.Next.IsNull ? 0 : 1, slotted.Count,
            slotted.CapacityLeft);

        if (Data.Next.IsNull == false)
        {
            new LeafPage(resolver.GetAt(Data.Next)).Report(reporter, resolver, level);
        }
    }
}