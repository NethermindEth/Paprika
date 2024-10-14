using System.Buffers;
using System.Collections.Specialized;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Paprika.Data;

namespace Paprika.Store;

/// <summary>
/// One of the bottom pages in the tree.
/// </summary>
[method: DebuggerStepThrough]
public readonly unsafe struct BottomPage(Page page) : IPage<BottomPage>
{
    private ref PageHeader Header => ref page.Header;

    private ref Payload Data => ref Unsafe.AsRef<Payload>(page.Payload);

    [StructLayout(LayoutKind.Explicit, Pack = sizeof(byte), Size = Size)]
    private struct Payload
    {
        private const int Size = Page.PageSize - PageHeader.Size;
        private const int DataOffset = DbAddress.Size;
        private const int DataSize = Size - DataOffset;

        [FieldOffset(0)]
        public DbAddress Next;

        /// <summary>
        /// The first item of map of frames to allow ref to it.
        /// </summary>
        [FieldOffset(DataOffset)]
        private byte DataStart;

        /// <summary>
        /// Writable area.
        /// </summary>
        public Span<byte> DataSpan => MemoryMarshal.CreateSpan(ref DataStart, DataSize);
    }

    public SlottedArray Map => new(Data.DataSpan);

    public void Accept(ref NibblePath.Builder builder, IPageVisitor visitor, IPageResolver resolver, DbAddress addr)
    {
        using var scope = visitor.On(ref builder, this, addr);
    }

    public Page Set(in NibblePath key, in ReadOnlySpan<byte> data, IBatchContext batch)
    {
        if (Header.BatchId != batch.BatchId)
        {
            return new BottomPage(batch.GetWritableCopy(page)).Set(key, data, batch);
        }

        var map = Map;

        if (data.IsEmpty)
        {
            // Delete, delete locally and in a child if it exists
            map.Delete(key);

            // Delete recursively, this might be later optimized with the tombstoning
            if (Data.Next.IsNull == false)
            {
                Data.Next = batch.GetAddress(new BottomPage(batch.GetAt(Data.Next)).Set(key, data, batch));
            }

            return page;
        }

        Debug.Assert(data.IsEmpty == false, "Should be an upsert, not a delete");

        // Try set directly
        if (map.TrySet(key, data))
        {
            return page;
        }

        // Page is full, ensure child
        var child = Data.Next.IsNull
            ? batch.GetNewPage<BottomPage>(out Data.Next, 0)
            : new BottomPage(batch.EnsureWritableCopy(ref Data.Next));

        map.MoveNonEmptyKeysTo(child.Map, true);

        // Try set again
        if (map.TrySet(key, data))
        {
            return page;
        }

        // Reuse this page for easier management and no need of copying it back in the parent.
        // 1. copy the content
        // 2. reuse the page
        // TODO: replace this with unmanaged pool of Paprika?
        var dataSpan = Data.DataSpan;
        var buffer = ArrayPool<byte>.Shared.Rent(dataSpan.Length);
        var copy = buffer.AsSpan(0, dataSpan.Length);

        dataSpan.CopyTo(copy);

        // All flushing failed, requires to move to a DataPage
        var destination = new DataPage(page);
        var p = destination.AsPage();
        p.Header.PageType = DataPage.DefaultType;
        p.Header.Metadata = default; // clear metadata
        destination.Clear();

        FlushToDataPage(destination, batch, new SlottedArray(copy), child);

        // All flushed, set the actual data now
        destination.Set(key, data, batch);

        ArrayPool<byte>.Shared.Return(buffer);

        // Register child for reuse
        batch.RegisterForFutureReuse(child.AsPage(), true);

        // The destination is set over this page.
        return page;
    }

    private static void FlushToDataPage(DataPage destination, IBatchContext batch, in SlottedArray map, in BottomPage child)
    {
        // Copy from the child first
        CopyToDestination(destination, child.Map, batch);

        // Copy all the entries from this
        CopyToDestination(destination, map, batch);
        return;

        static void CopyToDestination(DataPage destination, SlottedArray map, IBatchContext batch)
        {
            foreach (var item in map.EnumerateAll())
            {
                var result = new DataPage(destination.Set(item.Key, item.RawData, batch));

                Debug.Assert(result.AsPage().Raw == destination.AsPage().Raw, "Should not COW or replace the page");
            }
        }
    }

    public Page DeleteByPrefix(in NibblePath prefix, IBatchContext batch)
    {
        if (Header.BatchId != batch.BatchId)
        {
            // the page is from another batch, meaning, it's readonly. Copy
            var writable = batch.GetWritableCopy(page);
            return new BottomPage(writable).DeleteByPrefix(prefix, batch);
        }

        Map.DeleteByPrefix(prefix);

        // Child
        if (Data.Next.IsNull == false)
        {
            if (prefix.IsEmpty)
            {
                // Empty prefix, registered child for reuse
                batch.RegisterForFutureReuse(batch.GetAt(Data.Next), true);
                Data.Next = default;
            }
            else
            {
                new BottomPage(batch.EnsureWritableCopy(ref Data.Next)).DeleteByPrefix(prefix, batch);
            }
        }

        return page;
    }

    public void Clear()
    {
        Map.Clear();
        Data.Next = default;
    }

    [SkipLocalsInit]
    public bool TryGet(IPageResolver batch, scoped in NibblePath key, out ReadOnlySpan<byte> result)
    {
        if (Map.TryGet(key, out result))
            return true;

        if (key.IsEmpty)
            return false;

        var addr = Data.Next;
        if (addr.IsNull)
            return false;

        return new BottomPage(batch.GetAt(addr)).Map.TryGet(key, out result);
    }

    public static BottomPage Wrap(Page page) => Unsafe.As<Page, BottomPage>(ref page);

    public static PageType DefaultType => PageType.Bottom;

    public bool IsClean => Map.IsEmpty && Data.Next.IsNull;
}