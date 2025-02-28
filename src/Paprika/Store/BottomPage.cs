using System.Buffers;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using Paprika.Data;
using static Paprika.Data.NibbleSelector;
using Payload = Paprika.Store.DataPage.Payload;

namespace Paprika.Store;

/// <summary>
/// One of the bottom pages in the tree.
/// </summary>
[method: DebuggerStepThrough]
public readonly unsafe struct BottomPage(Page page) : IPage<BottomPage>
{
    private ref PageHeader Header => ref page.Header;

    private ref Payload Data => ref Unsafe.AsRef<Payload>(page.Payload);

    public SlottedArray Map => new(Data.DataSpan);

    public void Accept(ref NibblePath.Builder builder, IPageVisitor visitor, IPageResolver resolver, DbAddress addr)
    {
        using var scope = visitor.On(ref builder, this, addr);

        for (var i = 0; i < DataPage.BucketCount; i++)
        {
            var child = Data.Buckets[i];
            if (child.IsNull == false)
            {
                new BottomPage(resolver.GetAt(child)).Accept(ref builder, visitor, resolver, child);
            }
        }
    }

    public Page Set(in NibblePath key, in ReadOnlySpan<byte> data, IBatchContext batch)
    {
        Debug.Assert(batch.WasWritten(batch.GetAddress(page)), "All bottom pages should be COWed before use");

        var map = Map;

        if (data.IsEmpty)
        {
            Delete(key, batch);
            return page;
        }

        // Try setting value directly
        if (map.TrySet(key, data))
        {
            return page;
        }

        // Failed to add to map. Need to move data to child pages

        if (Data.Buckets[0].IsNull)
        {
            // The case where the first child was not allocated yet.
            var child = batch.GetNewPage<BottomPage>(out var childAddr, (byte)(Header.Level + 1));
            Data.Buckets[0] = childAddr;

            // Move all down. Ensure that deletes are treated as tombstones.
            map.MoveNonEmptyKeysTo<All>(child.Map, true);

            // All non-empty keys moved down. The map should is ready to accept the set.
            map.Set(key, data);

            return page;
        }

        var (existing, writtenThisBatch) = GatherChildrenInfo(batch);

        // If there are any children that were written this batch, try to write to them first.
        if (writtenThisBatch.HasAnySet)
        {
            // Flush down to the children written in this batch does not require COW as they were already copied during this batch.
            if (MoveToChildPages(map, batch, existing, false))
            {
                if (map.TrySet(key, data))
                {
                    return page;
                }
            }
        }

        // If there are any children that exist, but weren't written this batch.
        var childrenNotWrittenThisBatch = existing.AndNot(writtenThisBatch);
        if (childrenNotWrittenThisBatch.HasAnySet)
        {
            // There are children that were not COWed during this batch. Try to move using COW now.
            if (MoveToChildPages(map, batch, existing, true))
            {
                if (map.TrySet(key, data))
                {
                    return page;
                }
            }
        }

        // Ensure that all the children are created first before turning into the data page
        while (existing.HasAllSet == false)
        {
            if (!AllocateNewChild(batch, map))
            {
                // The child was not possible to allocate. Break and fall back to making it a data page.
                break;
            }

            if (map.TrySet(key, data))
            {
                return page;
            }

            var (e, _) = GatherChildrenInfo(batch);
            existing = e;
        }

        var destination = TurnToDataPage(batch);

        // All flushed, set the actual data now
        destination.Set(key, data, batch);

        // The destination is set over this page.
        return page;
    }

    [MethodImpl(MethodImplOptions.NoInlining)]
    private static void UnableToSet() => throw new Exception("The map should be ready to accept the write");

    private void Delete(in NibblePath key, IBatchContext batch)
    {
        var map = Map;

        var (children, _) = GatherChildrenInfo(batch);
        map.Delete(key);

        if (key.Length == 0)
            return;

        var at = GetExistingChildIndexWhereKeyBelongsTo(key, children);
        if (at == ChildNotFound)
        {
            // Nothing else to delete
            return;
        }

        var childAddr = Data.Buckets[at];
        var child = new BottomPage(batch.GetAt(childAddr));

        if (batch.WasWritten(childAddr))
        {
            // If the child was already written, no need to check whether it has a key or not.
            // It's cheaper to try to delete it rather than trying to get and then delete on positive.
            child.Map.Delete(key);
            return;
        }

        // The child was not written in this batch. First check whether it's worth to COW it.
        if (child.Map.Contains(key) == false)
        {
            return;
        }

        // The page contains the value but it was not COWed yet. COW and then delete.
        child = new BottomPage(batch.EnsureWritableCopy(ref childAddr));
        Data.Buckets[at] = childAddr;

        child.Map.Delete(key);
    }

    private (BitVector.Of256 existing, BitVector.Of256 writtenThisBatch) GatherChildrenInfo(IBatchContext batch)
    {
        BitVector.Of256 existing = default;
        BitVector.Of256 writtenThisBatch = default;

        for (var i = 0; i < DataPage.BucketCount; i++)
        {
            var addr = Data.Buckets[i];
            if (addr.IsNull == false)
            {
                existing[i] = true;
                writtenThisBatch[i] = batch.WasWritten(addr);
            }
        }

        return (existing, writtenThisBatch);
    }

    private bool AllocateNewChild(IBatchContext batch, in SlottedArray map)
    {
        // Gather size stats and find the biggest one that can help.
        Span<ushort> sizes = stackalloc ushort[256];
        map.GatherNonEmptySizeStats(sizes, static key => DataPage.GetBucket(key));

        var index = FindBestNotAllocatedChild(sizes, batch);
        if (index == ChildNotFound)
            return false;

        // Find the nibble that's child was previously matching.
        var previouslyMatching = FindMatchingChild((byte)index);

        Debug.Assert(previouslyMatching != ChildNotFound && Data.Buckets[previouslyMatching].IsNull == false,
            "There must be previously matching child. At least at 0th");

        // Allocate the new child
        Debug.Assert(Data.Buckets[index].IsNull);
        batch.GetNewPage<BottomPage>(out var addr, (byte)(Header.Level + 1));
        Data.Buckets[index] = addr;

        // Never flush down from the main map first to the child. It could be the case that it will have not enough space to handle data from the child on the left.
        // First, create the mask and migrate the data from the previously matching
        var childMask = new BitVector.Of256
        {
            [index] = true
        };

        // Migrate from the previously matching
        var prevAddr = Data.Buckets[previouslyMatching];
        var prevChild = new BottomPage(batch.EnsureWritableCopy(ref prevAddr));
        Data.Buckets[previouslyMatching] = prevAddr;

        // Pass the previous child as the source and construct the map to only point to the new child mask. Assert that everything what is needed is copied properly.
        MoveToChildPages(prevChild.Map, batch, childMask, false, true);

        // Only now try to move from the top to children.
        // Use all the child pages as the data were redistributed, but use only these that were written to. Pages that were not copied from were not modified and cannot be pushed to.
        var (_, written) = GatherChildrenInfo(batch);
        MoveToChildPages(map, batch, written, false);

        AssertChildrenRangeInvariant(batch);

        return true;
    }

    [Conditional("DEBUG")]
    private void AssertChildrenRangeInvariant(IBatchContext batch)
    {
        for (var i = 0; i < DataPage.BucketCount; i++)
        {
            var childAddress = Data.Buckets[i];
            if (childAddress.IsNull == false)
            {
                var child = new BottomPage(batch.GetAt(childAddress));
                foreach (var item in child.Map.EnumerateAll())
                {
                    Debug.Assert(item.Key.IsEmpty == false);
                }
            }
        }
    }

    private int FindBestNotAllocatedChild(Span<ushort> sizes, IBatchContext batch)
    {
        Debug.Assert(Data.Buckets[0].IsNull == false);

        var (allocatedMask, _) = GatherChildrenInfo(batch);

        int bestStart = ChildNotFound, bestTotal = 0;
        int curStart = ChildNotFound, curTotal = 0;

        // Single loop: track free ranges based solely on their start and cumulative weight.
        int i;

        for (i = 0; i < DataPage.BucketCount; i++)
        {
            if (allocatedMask[i] == false)
            {
                if (curStart == -1)
                    curStart = i;
                curTotal += sizes[i];
            }
            else
            {
                if (curStart != ChildNotFound && curTotal > bestTotal)
                {
                    bestStart = curStart;
                    bestTotal = curTotal;
                }

                curStart = ChildNotFound;
                curTotal = 0;
            }
        }

        if (curStart != ChildNotFound && curTotal > bestTotal)
        {
            bestStart = curStart;
            bestTotal = curTotal;
        }

        if (bestStart == ChildNotFound)
            return ChildNotFound;

        // While loop: iterate within the best free-range until reaching roughly half of its cumulative weight.
        var halfTotal = bestTotal / 2;
        var cumulative = 0;
        i = bestStart;

        while (i < DataPage.BucketCount && allocatedMask[i] == false)
        {
            cumulative += sizes[i];
            if (cumulative >= halfTotal)
                return i;
            i++;
        }

        return i - 1; // Return the last free nibble in the range.
    }

    private bool MoveToChildPages(in SlottedArray source, IBatchContext batch, BitVector.Of256 childIndexes, bool cow,
        bool assertAllCopied = false)
    {
        var moved = false;

        foreach (var item in source.EnumerateAll())
        {
            var k = item.Key;
            if (k.IsEmpty)
                continue;

            var at = GetExistingChildIndexWhereKeyBelongsTo(k, childIndexes);

            if (at < 0)
            {
                // No existing child that allows writing it down
                continue;
            }

            var addr = Data.Buckets[at];
            Debug.Assert(addr.IsNull == false);

            var written = batch.WasWritten(addr);

            if (written == false && !cow)
            {
                // This call is performed only on pages that were already COWed during this batch.
                // No cow allowance, continue
                continue;
            }

            if (written == false)
            {
                // copy the page as it was not written in this batch yet.
                batch.EnsureWritableCopy(ref addr);
                Data.Buckets[at] = addr;
            }

            var child = batch.GetAt(addr);
            Debug.Assert(batch.WasWritten(addr));

            var childMap = new BottomPage(child).Map;

            if (item.RawData.IsEmpty)
            {
                // It's a deletion, delete in original and in the child
                childMap.Delete(k);
                source.Delete(item);
                moved = true;
            }
            else if (childMap.TrySet(k, item.RawData))
            {
                // Successfully pushed down, delete
                source.Delete(item);
                moved = true;
            }
            else if (assertAllCopied)
            {
                Debug.Fail("Should always be able to set");
            }
        }

        return moved;
    }

    private static int GetExistingChildIndexWhereKeyBelongsTo(in NibblePath key, in BitVector.Of256 children)
    {
        return children.HighestSmallerOrEqualThan(DataPage.GetBucket(key));
    }

    private DataPage TurnToDataPage(IBatchContext batch)
    {
        // We need to turn this page into a full data page.
        // To make it work we need to ensure that invariant of the data page is preserved.
        // The invariant is that keys that are ShouldBeKeptLocal, should be kept in the data page locally
        // To ensure that this invariant is preserved, we copy the whole map of the main BottomPage and insert it later.
        // Additionally, when moving children, we do select these that should be kept locally as well.

        // Copy the main
        var required = Data.DataSpan.Length;
        var mainArray = ArrayPool<byte>.Shared.Rent(required);
        var mainBuffer = mainArray.AsSpan(0, required);
        Data.DataSpan.CopyTo(mainBuffer);
        var mainCopy = new SlottedArray(mainBuffer);
        var dp = new DataPage(page);

        // then clear the main
        new SlottedArray(Data.DataSpan).Clear();

        // The bottom page has the same layout as the DataPage. It can be directly turned into one.
        Header.PageType = DataPage.DefaultType;
        Header.Metadata = 0; // clear metadata

        // The child pages though are different because they don't have their prefix truncated.
        // Each of them needs to be turned into a bottom page with children truncated by one nibble.
        var array = ArrayPool<byte>.Shared.Rent(required);
        var buffer = array.AsSpan(0, required);
        var copy = new SlottedArray(buffer);

        for (var i = 0; i < DataPage.BucketCount; i++)
        {
            if (Data.Buckets[i].IsNull)
                continue;

            copy.Clear();

            var addr = Data.Buckets[i];
            var child = new BottomPage(batch.EnsureWritableCopy(ref addr));
            Data.Buckets[i] = addr;

            foreach (var item in child.Map.EnumerateAll())
            {
                if (DataPage.ShouldBeKeptLocal(item.Key))
                {
                    // The key should be kept locally, use the DataPage method to do it.
                    dp.Set(item.Key, item.RawData, batch);
                    continue;
                }

                var sliced = item.Key.SliceFrom(DataPage.ConsumedNibbles);
                var bucket = DataPage.GetBucket(item.Key);

                if (bucket == i)
                {
                    // A match on nibble, this is the value we should be writing to
                    copy.TrySet(sliced, item.RawData);
                }
                else
                {
                    // The case, where the nibble is from a child that was not created.
                    // Let's ensure it's created and move to the other child.
                    var otherAddr = Data.Buckets[bucket];
                    var otherChild = otherAddr.IsNull
                        ? batch.GetNewPage<BottomPage>(out otherAddr, (byte)(Header.Level + DataPage.ConsumedNibbles))
                        : new BottomPage(batch.EnsureWritableCopy(ref otherAddr));
                    Data.Buckets[bucket] = otherAddr;

                    // There will always be a place for it.
                    // If the nibble is up front, don't set the sliced data there as it will be reevaluated later.
                    otherChild.Map.TrySet(item.Key, item.RawData);
                }
            }

            // Copy back the span
            buffer.CopyTo(child.Data.DataSpan);
        }

        ArrayPool<byte>.Shared.Return(array);

        // Time to move the values from the copy of the main
        foreach (var item in mainCopy.EnumerateAll())
        {
            dp.Set(item.Key, item.RawData, batch);
        }

        ArrayPool<byte>.Shared.Return(mainArray);
        return dp;
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

        if (prefix.Length == 0)
        {
            for (var i = 0; i < DataPage.BucketCount; i++)
            {
                if (Data.Buckets[i].IsNull == false)
                {
                    batch.RegisterForFutureReuse(batch.GetAt(Data.Buckets[i]));
                }
            }

            Data.Buckets.Clear();
        }
        else
        {
            var i = FindMatchingChild(prefix);
            if (i != ChildNotFound)
            {
                var addr = Data.Buckets[i];
                var child = new BottomPage(batch.EnsureWritableCopy(ref addr));
                Data.Buckets[i] = addr;

                child.Map.DeleteByPrefix(prefix);
            }
        }

        return page;
    }

    public void Clear() => Data.Clear();

    [SkipLocalsInit]
    public bool TryGet(IPageResolver batch, scoped in NibblePath key, out ReadOnlySpan<byte> result)
    {
        if (Map.TryGet(key, out result))
            return true;

        if (key.IsEmpty)
            return false;

        var i = FindMatchingChild(key);


        return i != ChildNotFound && new BottomPage(batch.GetAt(Data.Buckets[i])).Map.TryGet(key, out result);
    }

    private const int ChildNotFound = -1;

    private int FindMatchingChild(in NibblePath key) => FindMatchingChild(DataPage.GetBucket(key));

    private int FindMatchingChild(int index)
    {
        var i = index;
        while (i >= 0 && Data.Buckets[i].IsNull)
        {
            i--;
        }

        return i;
    }

    public static BottomPage Wrap(Page page) => Unsafe.As<Page, BottomPage>(ref page);

    public static PageType DefaultType => PageType.Bottom;

    public bool IsClean => Data.IsClean;
}