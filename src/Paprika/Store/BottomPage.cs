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
        
        // Not enough space in bottom pages, create a DataPage
        var next = new DataPage(batch.GetNewPage(out var addr, true));
        next.Header.PageType = PageType.Standard;
        next.Header.Level = Header.Level;

        return CopyAndDestroy(next, batch);
    }

    private Page CopyAndDestroy(DataPage next, IBatchContext batch)
    {
        
    }

    private (bool success, Page cowed) TrySet(in NibblePath key, in ReadOnlySpan<byte> data, IBatchContext batch)
    {
        if (Header.BatchId != batch.BatchId)
        {
            // the page is from another batch, meaning, it's readonly. Copy
            var writable = batch.GetWritableCopy(page);
            return new BottomPage(writable).TrySet(key, data, batch);
        }
        
        if (Map.TrySet(key.RawSpan, data))
        {
            return (true, page);
        }

        if (BottomLevel >= MaxBottomLevel)
        {
            return (false, page);
        }

        // Try to set in map, on false try to make some space
        while (Map.TrySet(key.RawSpan, data) == false)
        {
            foreach (var item in Map.EnumerateAll())
            {
                var path = NibblePath.FromKey(item.Key);
                var bit = GetBit(path);
                
                // Lazily allocate pages only on finding that there's one to flush down
                var next = bit == 0 ? 
                    GetOrAllocChild(ref Data.Left, batch) : 
                    GetOrAllocChild(ref Data.Right, batch);

                var (success, cowed) = next.TrySet(path, item.RawData, batch);
                
                // Copy the address, if cowed
                if (bit == 0)
                {
                    Data.Left = batch.GetAddress(cowed);
                }
                else
                {
                    Data.Right = batch.GetAddress(cowed);
                }
                
                if (success)
                {
                    // One item moved, delete it and retry the insert in one more loop
                    Map.Delete(item);
                    break;
                }
            }
        }

        return (true, page);
    }

    /// <summary>
    /// Counter from 0, gives 0, 1, 2 levels which provide 7 pages.
    /// </summary>
    private const int MaxBottomLevel = 2;

    private BottomPage GetOrAllocChild(ref DbAddress addr, IBatchContext batch)
    {
        if (!addr.IsNull)
        {
            return new BottomPage(batch.GetAt(addr));
        }
        
        var child = new BottomPage(batch.GetNewPage(out addr, true));
        child.Header.PageType = PageType.Bottom;
        child.Header.Level = Header.Level;
        child.BottomLevel = (byte)(BottomLevel + 1);
        return child;
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

        reporter.ReportDataUsage(Header.PageType, level, Data.Next.IsNull ? 0 : 1, slotted.Count,
            slotted.CapacityLeft);

        if (Data.Next.IsNull == false)
        {
            new BottomPage(resolver.GetAt(Data.Next)).Report(reporter, resolver, level);
        }
    }
}