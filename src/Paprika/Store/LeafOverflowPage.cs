using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Paprika.Data;

namespace Paprika.Store;

/// <summary>
/// The page used to store big chunks of data.
/// </summary>
[method: DebuggerStepThrough]
public readonly unsafe struct LeafOverflowPage(Page page) : IPage
{
    private ref PageHeader Header => ref page.Header;

    private ref Payload Data => ref Unsafe.AsRef<Payload>(page.Payload);

    [StructLayout(LayoutKind.Explicit, Pack = sizeof(byte), Size = Size)]
    private struct Payload
    {
        private const int Size = Page.PageSize - PageHeader.Size;
        private const int DataSize = Size - DbAddress.Size;

        [FieldOffset(0)]
        public DbAddress Next;

        /// <summary>
        /// The first item of map of frames to allow ref to it.
        /// </summary>
        [FieldOffset(DbAddress.Size)]
        private byte DataStart;

        /// <summary>
        /// Writable area.
        /// </summary>
        public Span<byte> DataSpan => MemoryMarshal.CreateSpan(ref DataStart, DataSize);
    }

    public SlottedArray Map => new(Data.DataSpan);

    public void Clear()
    {
        Map.Clear();
        Data.Next = default;
    }

    public void Accept(ref NibblePath.Builder builder, IPageVisitor visitor, IPageResolver resolver, DbAddress addr)
    {
        using var scope = visitor.On(ref builder, this, addr);
    }

    public bool TryGet(in NibblePath key, out ReadOnlySpan<byte> result) => Map.TryGet(key, out result);

    public Page DeleteByPrefix(in NibblePath prefix, IBatchContext batch)
    {
        if (Header.BatchId != batch.BatchId)
        {
            // the page is from another batch, meaning, it's readonly. Copy
            var writable = batch.GetWritableCopy(page);
            return new LeafOverflowPage(writable).DeleteByPrefix(prefix, batch);
        }

        Map.DeleteByPrefix(prefix);

        return page;
    }

    public void Delete(in NibblePath key, IBatchContext batch)
    {
        Debug.Assert(batch.BatchId == Header.BatchId, "Should have been COWed before");
        Map.Delete(key);
    }

    public void StealFrom(in SlottedArray map)
    {
        map.MoveNonEmptyKeysTo(new MapSource(Map), treatEmptyAsTombstone: true);
    }

    public void RemoveKeysFrom(in SlottedArray map)
    {
        Map.RemoveKeysFrom(map);
    }

    public void GatherStats(Span<ushort> stats)
    {
        Map.GatherCountStats1Nibble(stats);
    }

    public void CopyTo(DataPage data, IBatchContext batch)
    {
        foreach (var item in Map.EnumerateAll())
        {
            var result = data.Set(item.Key, item.RawData, batch);

            Debug.Assert(result.Raw == data.AsPage().Raw, "Set should not change the data page");
        }
    }
}