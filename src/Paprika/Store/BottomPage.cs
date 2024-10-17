using System.Buffers;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Xml.Serialization;
using Paprika.Data;
using static Paprika.Data.NibbleSelector;

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

        [FieldOffset(0)] public DbAddress Left;
        [FieldOffset(DbAddress.Size)] public DbAddress Right;

        public bool HasAnyChildren => !Left.IsNull || !Right.IsNull;

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

        static void AcceptChildOrGrandChild(ref NibblePath.Builder builder, IPageVisitor visitor,
            IPageResolver resolver,
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

        // Go left, right first
        if (TryFlushDownToChild<HalfLow>(map, batch) && map.TrySet(key, data))
        {
            return page;
        }

        if (TryFlushDownToChild<HalfHigh>(map, batch) && map.TrySet(key, data))
        {
            return page;
        }

        // Four attempts for grand children, LL, LR, RL, RR
        if (TryFlushDownToGrandChildren<Q0, HalfLow>(map, batch) && map.TrySet(key, data))
        {
            return page;
        }

        if (TryFlushDownToGrandChildren<Q1, HalfLow>(map, batch) && map.TrySet(key, data))
        {
            return page;
        }

        if (TryFlushDownToGrandChildren<Q2, HalfHigh>(map, batch) && map.TrySet(key, data))
        {
            return page;
        }

        if (TryFlushDownToGrandChildren<Q3, HalfHigh>(map, batch) && map.TrySet(key, data))
        {
            return page;
        }

        var destination = TurnToDataPage(batch);

        // All flushed, set the actual data now
        destination.Set(key, data, batch);

        // The destination is set over this page.
        return page;
    }

    private DataPage TurnToDataPage(IBatchContext batch)
    {
        // Capture descendants addresses
        Span<DbAddress> descendants = stackalloc DbAddress[6];

        SetDescendants(descendants, Data.Left, batch);
        SetDescendants(descendants[3..], Data.Right, batch);

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

        ArrayPool<byte>.Shared.Return(buffer);

        return destination;

        static void SetDescendants(Span<DbAddress> descendants, DbAddress childAddr, IPageResolver batch)
        {
            descendants[0] = childAddr;
            if (childAddr.IsNull)
                return;

            var child = new BottomPage(batch.GetAt(childAddr));

            descendants[1] = child.Data.Left;
            descendants[2] = child.Data.Right;
        }
    }

    /// <summary>
    /// Tries to flush down data one level, to the child level.
    /// </summary>
    private bool TryFlushDownToChild<TChildSelector>(in SlottedArray map, IBatchContext batch)
        where TChildSelector : INibbleSelector
    {
        ref var addr = ref typeof(TChildSelector) == typeof(HalfLow) ? ref Data.Left : ref Data.Right;

        if (map.HasAny<TChildSelector>() == false)
        {
            // No reason to flush if map contains no data with the given selector
            return false;
        }

        var child = EnsureWritable(ref addr, batch);
        var treatEmptyAsTombstone = !child.Data.HasAnyChildren;
        return map.MoveNonEmptyKeysTo<TChildSelector>(child.Map, treatEmptyAsTombstone);
    }

    private bool TryFlushDownToGrandChildren<TGrandChildSelector, TChildSelector>(in SlottedArray map, IBatchContext batch)
        where TGrandChildSelector : INibbleSelector<TChildSelector>
        where TChildSelector : INibbleSelector
    {
        // This execution follows upt TryFlushDownToChild, meaning that map may have no data for the child
        // If there is no data for the given child, there's no reason to push it down as it will change nothing in the map.
        // Check for the selector first then.
        if (map.HasAny<TChildSelector>() == false)
        {
            // No reason to flush if map contains no data with the given selector
            return false;
        }

        ref var addr = ref typeof(TChildSelector) == typeof(HalfLow) ? ref Data.Left : ref Data.Right;

        // There are some data that belong to the super set, but they were unable to be flushed to the child map.
        // Try to copy to grand children first, then back from map to child map.
        var child = EnsureWritable(ref addr, batch);

        // Check if map or the child has any keys
        if (!map.HasAny<TGrandChildSelector>() && !child.Map.HasAny<TGrandChildSelector>())
            return false;

        ref var grandChildAddr = ref TGrandChildSelector.Low ? ref child.Data.Left : ref child.Data.Right;
        var grandChild = EnsureWritable(ref grandChildAddr, batch);

        // Remove keys that are in the top map first. They will be overwritten anyway.
        child.Map.RemoveKeysFrom(map);
        grandChild.Map.RemoveKeysFrom(map);

        // This is the last level, treat empty as tombstones now
        if (child.Map.MoveNonEmptyKeysTo<TGrandChildSelector>(grandChild.Map, true))
        {
            // Return whether map got some more space
            return map.MoveNonEmptyKeysTo<TChildSelector>(child.Map, false);
        }

        return false;
    }

    private static BottomPage EnsureWritable(ref DbAddress addr, IBatchContext batch)
    {
        return addr.IsNull
            ? batch.GetNewCleanPage<BottomPage>(out addr, 0)
            : new BottomPage(batch.EnsureWritableCopy(ref addr));
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

        var nibble = key.Nibble0;

        var addr = HalfLow.Should(nibble) ? Data.Left : Data.Right;
        if (addr.IsNull)
            return false;

        // search through child
        var child = new BottomPage(batch.GetAt(addr));
        if (child.Map.TryGet(key, out result))
            return true;

        DbAddress grandChildAddr;

        if (HalfLow.Should(nibble))
        {
            grandChildAddr = Q0.Should(nibble) ? child.Data.Left : child.Data.Right;
        }
        else
        {
            grandChildAddr = Q2.Should(nibble) ? child.Data.Left : child.Data.Right;
        }

        if (grandChildAddr.IsNull)
            return false;

        // search through grand-child
        return new BottomPage(batch.GetAt(grandChildAddr)).Map.TryGet(key, out result);
    }

    public static BottomPage Wrap(Page page) => Unsafe.As<Page, BottomPage>(ref page);

    public static PageType DefaultType => PageType.Bottom;

    public bool IsClean => Map.IsEmpty && Data.Left.IsNull && Data.Right.IsNull;
}