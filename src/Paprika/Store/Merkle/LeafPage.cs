using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Paprika.Data;

namespace Paprika.Store.Merkle;

/// <summary>
/// A Merkle leaf page that keeps two levels of Merkle data in <see cref="Payload.MerkleNodes"/>
/// while the rest in <see cref="Map"/>. Once it cannot contain the data, it turns into a <see cref="FanOutPage"/>.  
/// </summary>
/// <param name="page"></param>
[method: DebuggerStepThrough]
public readonly unsafe struct LeafPage(Page page) : IPageWithData<LeafPage>, IClearable
{
    public static LeafPage Wrap(Page page) => Unsafe.As<Page, LeafPage>(ref page);

    private ref PageHeader Header => ref page.Header;

    private ref Payload Data => ref Unsafe.AsRef<Payload>(page.Payload);

    public void Clear()
    {
        Data.MerkleNodes.Clear();
        Map.Clear();
    }

    public Page Set(in NibblePath key, in ReadOnlySpan<byte> data, IBatchContext batch)
    {
        if (Header.BatchId != batch.BatchId)
        {
            // the page is from another batch, meaning, it's readonly. Copy
            var writable = batch.GetWritableCopy(page);
            return new LeafPage(writable).Set(key, data, batch);
        }

        if (Data.MerkleNodes.TrySet(key, data, batch))
        {
            return page;
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

        var updated = new FanOutPage(@new);

        // Set values from map
        foreach (var item in Map.EnumerateAll())
        {
            updated = new FanOutPage(updated.Set(item.Key, item.RawData, batch));
        }

        // Set values from Merkle
        foreach (var item in Data.MerkleNodes.EnumerateAll(batch))
        {
            updated = new FanOutPage(updated.Set(item.Key, item.RawData, batch));
        }

        // Recycle pages
        Data.MerkleNodes.RegisterForFutureReuse(batch);

        // Set this value and return data page
        return updated.Set(key, data, batch);
    }

    public bool TryGet(IReadOnlyBatchContext batch, scoped in NibblePath key, out ReadOnlySpan<byte> result)
    {
        if (Data.MerkleNodes.TryGet(key, out result, batch))
            return true;

        // Try search write-through data
        return new SlottedArray(Data.DataSpan).TryGet(key, out result);
    }

    /// <summary>
    /// Represents the data of this data page. This type of payload stores data in 16 nibble-addressable buckets.
    /// These buckets are used to store up to <see cref="DataSize"/> entries before flushing them down as other pages
    /// like page split.
    /// </summary>
    [StructLayout(LayoutKind.Explicit, Size = Size)]
    private struct Payload
    {
        private const int Size = Page.PageSize - PageHeader.Size;

        /// <summary>
        /// The size of the raw byte data held in this page. Must be long aligned.
        /// </summary>
        private const int DataSize = Size - MerkleNodes.Size;

        private const int DataOffset = Size - DataSize;

        [FieldOffset(0)] public MerkleNodes MerkleNodes;

        /// <summary>
        /// The first item of map of frames to allow ref to it.
        /// </summary>
        [FieldOffset(DataOffset)] private byte DataStart;

        /// <summary>
        /// Writable area.
        /// </summary>
        public Span<byte> DataSpan => MemoryMarshal.CreateSpan(ref DataStart, DataSize);
    }

    private SlottedArray Map => new(Data.DataSpan);

    public void Report(IReporter reporter, IPageResolver resolver, int pageLevel, int trimmedNibbles)
    {
        var slotted = new SlottedArray(Data.DataSpan);
        reporter.ReportDataUsage(Header.PageType, pageLevel, trimmedNibbles, slotted);
    }

    public void Accept(IPageVisitor visitor, IPageResolver resolver, DbAddress addr)
    {
        using var scope = visitor.On(this, addr);
    }
}