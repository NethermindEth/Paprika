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

        if (Data.Next.IsNull)
            return;

        new BottomPage(resolver.GetAt(Data.Next)).Accept(ref builder, visitor, resolver, Data.Next);
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

        child.MoveNonEmptyKeysFrom(map, batch);

        // Try set again
        if (map.TrySet(key, data))
        {
            return page;
        }

        // At this point, moving to the child failed to get more space.
        // It's time to check if the child has a child, if not, create a link.
        if (child.Data.Next.IsNull)
        {
            // Let's create an additional page between this page and its child. This will be 2 levels of descendants.
            var current = Data.Next;
            child = batch.GetNewPage<BottomPage>(out Data.Next);
            child.Data.Next = current;

            // Now, the new child has space, let's try to move again.
            child.MoveNonEmptyKeysFrom(map, batch);

            // Try set again
            if (map.TrySet(key, data))
            {
                return page;
            }
        }

        // Capture descendants addresses
        Span<DbAddress> descendants = stackalloc DbAddress[2];
        descendants[0] = Data.Next;
        descendants[1] = new BottomPage(batch.GetAt(Data.Next)).Data.Next;

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

        FlushToDataPage(destination, batch, new SlottedArray(copy), descendants);

        // All flushed, set the actual data now
        destination.Set(key, data, batch);

        ArrayPool<byte>.Shared.Return(buffer);

        // Register descendants for reuse
        foreach (var descendant in descendants)
        {
            batch.RegisterForFutureReuse(batch.GetAt(descendant), true);
        }

        // The destination is set over this page.
        return page;
    }

    private void MoveNonEmptyKeysFrom(in SlottedArray source, IBatchContext batch)
    {
        Debug.Assert(batch.WasWritten(batch.GetAddress(page)), "This page should have been COWed");

        if (Data.Next.IsNull == false)
        {
            // Move to child first
            var child = new BottomPage(batch.EnsureWritableCopy(ref Data.Next));
            child.MoveNonEmptyKeysFrom(Map, batch);
        }

        // Move to this Map
        source.MoveNonEmptyKeysTo(Map, true);
    }

    private static void FlushToDataPage(DataPage destination, IBatchContext batch, in SlottedArray map, in Span<DbAddress> descendants)
    {
        // The ordering of descendants is important. We start with the most nested ones
        for (var i = descendants.Length - 1; i >= 0; i--)
        {
            var descendant = new BottomPage(batch.GetAt(descendants[i]));
            CopyToDestination(destination, descendant.Map, batch);
        }

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
            new BottomPage(batch.EnsureWritableCopy(ref Data.Next)).DeleteByPrefix(prefix, batch);
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

        var addr = Data.Next;
        if (addr.IsNull)
            return false;

        // Recursive call
        return new BottomPage(batch.GetAt(addr)).TryGet(batch, key, out result);
    }

    public static BottomPage Wrap(Page page) => Unsafe.As<Page, BottomPage>(ref page);

    public static PageType DefaultType => PageType.Bottom;

    public bool IsClean => Map.IsEmpty && Data.Next.IsNull;
}