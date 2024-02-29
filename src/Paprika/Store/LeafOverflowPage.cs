using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Paprika.Data;

namespace Paprika.Store;

/// <summary>
/// The page used to store big chunks of data.
/// </summary>
[method: DebuggerStepThrough]
public readonly unsafe struct LeafOverflowPage(Page page)
{
    private ref PageHeader Header => ref page.Header;

    private ref Payload Data => ref Unsafe.AsRef<Payload>(page.Payload);

    public bool CanStore(in ReadOnlySpan<byte> data) => Map.CanAdd(data);

    public (Page, ushort) Add(in ReadOnlySpan<byte> data, IBatchContext batch)
    {
        if (Map.CanAdd(data) == false || Data.Ids.HasEmptyBits == false)
        {
            throw new Exception("Cannot store more data!");
        }

        if (Header.BatchId != batch.BatchId)
        {
            // the page is from another batch, meaning, it's readonly. Copy
            var writable = batch.GetWritableCopy(page);
            return new LeafOverflowPage(writable).Add(data, batch);
        }

        // get id
        var id = Data.Ids.FirstNotSet;
        if (Map.TrySet(id, data))
        {
            Data.Ids[id] = true;
            return (page, id);
        }

        throw new Exception("Cannot store more data!");
    }

    [StructLayout(LayoutKind.Explicit, Size = Size)]
    private struct Payload
    {
        private const int Size = Page.PageSize - PageHeader.Size - BitVector1024.Size;

        [FieldOffset(0)] public BitVector1024 Ids;

        /// <summary>
        /// The first item of map of frames to allow ref to it.
        /// </summary>
        [FieldOffset(BitVector1024.Size)] private byte DataStart;

        /// <summary>
        /// Writable area.
        /// </summary>
        public Span<byte> DataSpan => MemoryMarshal.CreateSpan(ref DataStart, Size);
    }

    public bool TryGet(ushort key, out ReadOnlySpan<byte> result) => Map.TryGet(key, out result);

    private UShortSlottedArray Map => new(Data.DataSpan);

    public int CapacityLeft => Map.CapacityLeft;

    public Page Delete(ushort key, IBatchContext batch)
    {
        if (Header.BatchId != batch.BatchId)
        {
            // the page is from another batch, meaning, it's readonly. Copy
            var writable = batch.GetWritableCopy(page);
            return new LeafOverflowPage(writable).Delete(key, batch);
        }

        // Deletes are in-situ
        Map.Delete(key);
        Data.Ids[key] = false; // set id free

        return page;
    }

    public void Accept(IPageVisitor visitor, IPageResolver resolver, DbAddress addr)
    {
        using var scope = visitor.On(this, addr);
    }

    public void Report(IReporter reporter, IPageResolver resolver, int level)
    {
        reporter.ReportDataUsage(Header.PageType, level, 0, Map.Count, Map.CapacityLeft);
    }
}