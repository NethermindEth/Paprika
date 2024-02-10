using System.Buffers;
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

    private bool UseThis(in NibblePath key) => key.IsEmpty || Data.Next.IsNull || key.GetAt(0) % 2 == 0;

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
            var (success, cowedPage) = TrySetInSitu(key, data, batch);
            if (success)
            {
                return cowedPage;
            }

            // Not enough space, see whether can partition
            if (Data.Next.IsNull)
            {
                throw new Exception("Partition");
            }
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

            throw new Exception("Filled! Split to DataPage!");
        }

        // The page has some of the values flushed down, try to add again.
        return Set(key, data, batch);
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

        return useThis
            ? Map.TryGet(key.RawSpan, out result)
            : new LeafPage(batch.GetAt(Data.Next)).TryGet(key, batch, out result);
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