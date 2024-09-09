using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Xml.Serialization;
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

        [FieldOffset(0)] public DbAddress Next;

        /// <summary>
        /// The first item of map of frames to allow ref to it.
        /// </summary>
        [FieldOffset(DbAddress.Size)] private byte DataStart;

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

        if (Data.Next.IsNull == false)
        {
            new LeafOverflowPage(resolver.GetAt(Data.Next)).Accept(ref builder, visitor, resolver, addr);
        }
    }

    public bool TryGet(scoped in NibblePath key, IReadOnlyBatchContext batch, out ReadOnlySpan<byte> result)
    {
        if (Map.TryGet(key, out result))
        {
            return true;
        }

        if (Data.Next.IsNull == false)
        {
            return new LeafOverflowPage(batch.GetAt(Data.Next)).TryGet(key, batch, out result);
        }

        return false;
    }

    public Page DeleteByPrefix(in NibblePath prefix, IBatchContext batch)
    {
        if (Header.BatchId != batch.BatchId)
        {
            // the page is from another batch, meaning, it's readonly. Copy
            var writable = batch.GetWritableCopy(page);
            return new LeafOverflowPage(writable).DeleteByPrefix(prefix, batch);
        }

        Map.DeleteByPrefix(prefix);

        if (Data.Next.IsNull == false)
        {
            Data.Next = batch.GetAddress(new LeafOverflowPage(batch.GetAt(Data.Next)).DeleteByPrefix(prefix, batch));
        }

        return page;
    }

    public Page Delete(in NibblePath key, IBatchContext batch)
    {
        Debug.Assert(batch.BatchId == Header.BatchId, "Should have been COWed before");
        Map.Delete(key);

        if (Data.Next.IsNull == false)
        {
            Data.Next = batch.GetAddress(new LeafOverflowPage(batch.GetAt(Data.Next)).Delete(key, batch));
        }

        return page;
    }

    public bool MoveFromThenTrySet(in SlottedArray parent, in NibblePath key, ReadOnlySpan<byte> data,
        IBatchContext batch)
    {
        parent.MoveNonEmptyKeysTo(Map);

        // After moving, try set in the original map
        if (parent.TrySet(key, data))
            return true;

        // Failed to set in parent, try set in Map,  first ensure that no stale version is above
        parent.Delete(key);
        if (Map.TrySet(key, data))
            return true;

        Debug.Assert(parent.Contains(key) == false, "Parent should contain the key");

        // No space in the map of the page above, no space in the map of this page. Ensure one more level
        LeafOverflowPage child;

        if (Data.Next.IsNull)
        {
            // No level below, create one
            child = new LeafOverflowPage(batch.GetNewPage(out Data.Next, false));
            ref var header = ref child.AsPage().Header;

            header.PageType = PageType.LeafOverflow;
            header.Level = Header.Level; // same level

            child.Clear();
        }
        else
        {
            child = new LeafOverflowPage(batch.EnsureWritableCopy(ref Data.Next));
        }

        // Move from this to the child, so that some space is left
        Map.MoveNonEmptyKeysTo(child.Map);

        if (Map.TrySet(key, data))
            return true;

        // Failed to set in Map, try set in child, first ensure that no stale version in Map
        Map.Delete(key);
        if (child.Map.TrySet(key, data))
            return true;

        // No overflowing or moving helped
        return false;
    }

    public Page RemoveKeysFrom(in SlottedArray map, IBatchContext batch)
    {
        Map.RemoveKeysFrom(map);

        if (Data.Next.IsNull == false)
        {
            Data.Next = batch.GetAddress(new LeafOverflowPage(batch.GetAt(Data.Next)).RemoveKeysFrom(map, batch));
        }

        return page;
    }

    public void GatherStats(Span<ushort> stats, IBatchContext batch)
    {
        Map.GatherCountStats1Nibble(stats);

        if (Data.Next.IsNull == false)
        {
            new LeafOverflowPage(batch.GetAt(Data.Next)).GatherStats(stats, batch);
        }
    }

    public void CopyToThenReuse(DataPage data, IBatchContext batch)
    {
        // First, copy the next as it can have stale data
        if (Data.Next.IsNull == false)
        {
            new LeafOverflowPage(batch.GetAt(Data.Next)).CopyToThenReuse(data, batch);
        }

        // Then copy from this page
        foreach (var item in Map.EnumerateAll())
        {
            var result = data.Set(item.Key, item.RawData, batch);
            Debug.Assert(result.Raw == data.AsPage().Raw, "Set should not change the data page");
        }

        batch.RegisterForFutureReuse(page, possibleImmediateReuse: true);
    }
}