using System.Buffers;
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
        private const int BucketSize = DbAddress.Size * 2;

        /// <summary>
        /// The size of the raw byte data held in this page. Must be long aligned.
        /// </summary>
        private const int DataSize = Size - BucketSize;

        private const int DataOffset = Size - DataSize;

        [FieldOffset(0)] public DbAddress Grand;

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

    public SlottedArray Map => new(Data.DataSpan);

    public void Accept(ref NibblePath.Builder builder, IPageVisitor visitor, IPageResolver resolver, DbAddress addr)
    {
        using var scope = visitor.On(ref builder, this, addr);

        AcceptChildOrGrandChild(ref builder, visitor, resolver, Data.Left);
        AcceptChildOrGrandChild(ref builder, visitor, resolver, Data.Right);

        return;

        static void AcceptChildOrGrandChild(ref NibblePath.Builder builder, IPageVisitor visitor, IPageResolver resolver,
            DbAddress addr)
        {
            if (addr.IsNull == false)
            {
                new BottomPage(resolver.GetAt(addr)).Accept(ref builder, visitor, resolver, addr);
            }
        }
    }

    public Page Set(in NibblePath key, in ReadOnlySpan<byte> data, IBatchContext batch)
    {
        if (Header.BatchId != batch.BatchId)
        {
            return new BottomPage(batch.GetWritableCopy(page)).Set(key, data, batch);
        }

        var map = Map;

        if (data.IsEmpty && ((Data.Left.IsNull && Data.Right.IsNull) || key.IsEmpty))
        {
            // Delete with no children can be executed immediately, also empty key
            map.Delete(key);
            return page;
        }

        // Try setting value directly
        if (map.TrySet(key, data))
        {
            return page;
        }

        // Go left, right, then grand-child left, grand-child right
        if (TryFlushDown<LowerHalfSelector>(map, batch, false) && map.TrySet(key, data))
        {
            return page;
        }

        if (TryFlushDown<UpperHalfSelector>(map, batch, false) && map.TrySet(key, data))
        {
            return page;
        }

        if (TryFlushDown<LowerHalfSelector>(map, batch, true) && map.TrySet(key, data))
        {
            return page;
        }

        if (TryFlushDown<UpperHalfSelector>(map, batch, true) && map.TrySet(key, data))
        {
            return page;
        }

        // Capture descendants addresses
        Span<DbAddress> descendants =
            [Data.Left, Data.Right, GetGrandOrDefault(Data.Left, batch), GetGrandOrDefault(Data.Right, batch)];

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

        // The destination is set over this page.
        return page;

        static DbAddress GetGrandOrDefault(DbAddress addr, IPageResolver batch) =>
            addr.IsNull ? DbAddress.Null : new BottomPage(batch.GetAt(addr)).Data.Grand;
    }

    /// <summary>
    /// Tries to flush down data to descendants.
    /// </summary>
    private bool TryFlushDown<TSelector>(in SlottedArray map, IBatchContext batch, bool grandChildren)
        where TSelector : INibbleSelector
    {
        ref var addr = ref typeof(TSelector) == typeof(LowerHalfSelector) ? ref Data.Left : ref Data.Right;

        if (map.HasAny<TSelector>() == false)
        {
            // no reason to flush if map contains no data with the given selector
            return false;
        }

        var child = Ensure(ref addr, batch);

        var deleteTombstonesOnlyIfNoGrandChild = child.Data.Grand.IsNull;
        var moved = map.MoveNonEmptyKeysTo<TSelector>(child.Map, deleteTombstonesOnlyIfNoGrandChild);

        // grand children not allowed and no grand child
        ref var grandAddr = ref child.Data.Grand;

        if (!grandChildren && grandAddr.IsNull)
            return moved;

        var grand = Ensure(ref grandAddr, batch);

        // Clean from grand first that are in the map, they are overwritten anyway
        grand.Map.RemoveKeysFrom(map);

        // Move from child to grand
        child.Map.MoveNonEmptyKeysTo<AllNibblesSelector>(grand.Map, true);

        // Move from map to child now. No tombstones removal though as there's a grandchild.
        return map.MoveNonEmptyKeysTo<TSelector>(child.Map, false);

        static BottomPage Ensure(ref DbAddress addr, IBatchContext batch)
        {
            return addr.IsNull
                ? batch.GetNewCleanPage<BottomPage>(out addr, 0)
                : new BottomPage(batch.EnsureWritableCopy(ref addr));
        }
    }

    private static void FlushToDataPage(DataPage destination, IBatchContext batch, in SlottedArray map,
        in Span<DbAddress> descendants)
    {
        // The ordering of descendants might be important. Start with the most nested ones first.
        for (var i = descendants.Length - 1; i >= 0; i--)
        {
            if (descendants[i].IsNull)
            {
                continue;
            }

            var page = batch.GetAt(descendants[i]);
            var descendant = new BottomPage(page);
            CopyToDestination(destination, descendant.Map, batch);

            // Already copied, register for immediate reuse
            batch.RegisterForFutureReuse(page, true);
        }

        // Copy all the entries from this
        CopyToDestination(destination, map, batch);
        return;

        static void CopyToDestination(DataPage destination, in SlottedArray map, IBatchContext batch)
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

        DeleteByPrefixImpl(ref Data.Left, prefix, batch);
        DeleteByPrefixImpl(ref Data.Right, prefix, batch);

        return page;

        static void DeleteByPrefixImpl(ref DbAddress addr, in NibblePath prefix, IBatchContext batch)
        {
            if (addr.IsNull)
                return;

            var child = new BottomPage(batch.EnsureWritableCopy(ref addr));
            child.DeleteByPrefix(prefix, batch);
        }
    }

    public void Clear()
    {
        Map.Clear();
        Data.Left = default;
        Data.Right = default;
    }

    [SkipLocalsInit]
    public bool TryGet(IPageResolver batch, scoped in NibblePath key, out ReadOnlySpan<byte> result)
    {
        if (Map.TryGet(key, out result))
            return true;

        if (key.IsEmpty)
            return false;

        var addr = LowerHalfSelector.Should(key.Nibble0) ? Data.Left : Data.Right;
        if (addr.IsNull)
            return false;

        // search through child
        var child = new BottomPage(batch.GetAt(addr));
        if (child.Map.TryGet(key, out result))
            return true;

        if (child.Data.Grand.IsNull)
            return false;

        // search through grand-child
        return new BottomPage(batch.GetAt(child.Data.Grand)).Map.TryGet(key, out result);
    }

    public static BottomPage Wrap(Page page) => Unsafe.As<Page, BottomPage>(ref page);

    public static PageType DefaultType => PageType.Bottom;

    public bool IsClean => Map.IsEmpty && Data.Left.IsNull && Data.Right.IsNull;
}