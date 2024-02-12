using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Paprika.Data;

namespace Paprika.Store;

/// <summary>
/// Represents the leaf page that can be split in two.
/// </summary>
[method: DebuggerStepThrough]
public readonly unsafe struct BottomPage(Page page) : IPageWithData<BottomPage>
{
    private const int Left = 0;
    private const int Right = 1;

    public static BottomPage Wrap(Page page) => new(page);

    private ref PageHeader Header => ref page.Header;

    private ref Payload Data => ref Unsafe.AsRef<Payload>(page.Payload);

    private ref byte BottomLevel => ref Header.Metadata;

    public Page Set(in NibblePath key, in ReadOnlySpan<byte> data, IBatchContext batch)
    {
        var (success, cowed) = TrySet(key, data, batch);

        if (success)
        {
            return cowed;
        }

        // Not enough space in bottom pages, create a DataPage and put all existing data there
        var next = new DataPage(batch.GetNewPage(out _, true));
        next.Header.PageType = PageType.Standard;
        next.Header.Level = Header.Level;
        next = new DataPage(CopyAndDestroy(next, batch));

        // Now add the new key
        return next.Set(key, data, batch);
    }

    private Page CopyAndDestroy(DataPage next, IBatchContext batch)
    {
        // Copy all
        foreach (var item in Map.EnumerateAll())
        {
            var path = NibblePath.FromKey(item.Key).SliceFrom(Header.LevelOddity);
            next = new DataPage(next.Set(path, item.RawData, batch));
        }

        // Register for destruction
        batch.RegisterForFutureReuse(page);

        // Call for left
        if (Data.Left.IsNull == false)
        {
            next = new DataPage(new BottomPage(batch.GetAt(Data.Left)).CopyAndDestroy(next, batch));
        }

        if (Data.Right.IsNull == false)
        {
            next = new DataPage(new BottomPage(batch.GetAt(Data.Right)).CopyAndDestroy(next, batch));
        }

        return next.AsPage();
    }

    private (bool success, Page cowed) TrySet(in NibblePath key, in ReadOnlySpan<byte> data, IBatchContext batch)
    {
        if (Header.BatchId != batch.BatchId)
        {
            // COW
            var writable = batch.GetWritableCopy(page);
            return new BottomPage(writable).TrySet(key, data, batch);
        }

        // Try in-page write
        if (Map.TrySet(key.RawSpan, data))
        {
            return (true, page);
        }

        if (BottomLevel >= MaxBottomLevel)
        {
            return (false, page);
        }

        // No space in this page. We need to push some data down.
        // First, we'll flush left-wing then right-wing.
        if (TryWriteAndFlush(key, data, batch, Left))
        {
            return (true, page);
        }

        if (TryWriteAndFlush(key, data, batch, Right))
        {
            return (true, page);
        }

        return (false, page);
    }

    private bool TryWriteAndFlush(in NibblePath key, ReadOnlySpan<byte> data, IBatchContext batch, int child)
    {
        while (Map.TrySet(key.RawSpan, data) == false)
        {
            if (TryFlushChild(batch, child) == false)
            {
                // The child was unable to accept the flush. Report failure on using this one.
                return false;
            }
        }

        // The value has been set locally.
        return true;
    }

    private bool TryFlushChild(IBatchContext batch, int child)
    {
        ref var addr = ref child == Left ? ref Data.Left : ref Data.Right;

        var next = GetOrAllocChild(ref addr, batch);

        foreach (var item in Map.EnumerateAll())
        {
            var path = NibblePath.FromKey(item.Key).SliceFrom(Header.LevelOddity);
            var bit = GetBit(path);

            if (bit != child)
            {
                continue;
            }

            var (success, cowed) = next.TrySet(path, item.RawData, batch);
            addr = batch.GetAddress(cowed);

            if (success)
            {
                // One item moved, delete it and retry the insert in one more loop
                Map.Delete(item);
                return true;
            }
        }

        return false;
    }

    /// <summary>
    /// Counter from 0, gives 0, 1, 2 levels which provide 7 pages.
    /// </summary>
    private const int MaxBottomLevel = 2;

    private BottomPage GetOrAllocChild(ref DbAddress addr, IBatchContext batch)
    {
        if (!addr.IsNull)
        {
            var existing = batch.GetAt(addr);
            Debug.Assert(existing.Header.PageType == PageType.Bottom);

            return new BottomPage(existing);
        }

        var child = new BottomPage(batch.GetNewPage(out addr, true));
        child.Header.PageType = PageType.Bottom;
        child.Header.Level = Header.Level;
        child.BottomLevel = (byte)(BottomLevel + 1);
        return child;
    }

    [StructLayout(LayoutKind.Explicit, Size = Size)]
    private struct Payload
    {
        private const int Size = Page.PageSize - PageHeader.Size;

        /// <summary>
        /// The size of the raw byte data held in this page. Must be long aligned.
        /// </summary>
        private const int DataSize = Size - DataOffset;

        private const int DataOffset = DbAddress.Size * 2;

        [FieldOffset(0)] public DbAddress Left;

        [FieldOffset(DbAddress.Size)] public DbAddress Right;

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

        if (Map.TryGet(key.RawSpan, out result))
        {
            return true;
        }

        if (key.IsEmpty)
        {
            return false;
        }

        var bit = GetBit(key);

        var next = bit == 0 ? Data.Left : Data.Right;

        if (next.IsNull == false)
        {
            return new BottomPage(batch.GetAt(next)).TryGet(key, batch, out result);
        }

        return false;
    }

    /// <summary>
    /// Gets the left or right bit.
    /// </summary>
    private int GetBit(in NibblePath key) => (key.GetAt(Header.LevelOddity) >> BottomLevel) & 1;

    private SlottedArray Map => new(Data.DataSpan);

    public void Report(IReporter reporter, IPageResolver resolver, int level)
    {
        var slotted = new SlottedArray(Data.DataSpan);

        reporter.ReportDataUsage(Header.PageType, level, 0, slotted.Count,
            slotted.CapacityLeft);

        if (Data.Left.IsNull == false)
        {
            new BottomPage(resolver.GetAt(Data.Left)).Report(reporter, resolver, level);
        }

        if (Data.Right.IsNull == false)
        {
            new BottomPage(resolver.GetAt(Data.Right)).Report(reporter, resolver, level);
        }
    }
}