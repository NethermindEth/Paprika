using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Paprika.Data;

namespace Paprika.Store;

/// <summary>
/// The fan out page for storage, buffering some writes and providing a heavy fan out.
/// Once filled, flushed down all its content to make room for more.
/// Useful only for higher levels of the storage trie.
/// </summary>
[method: DebuggerStepThrough]
public readonly unsafe struct StorageFanOutPage<TNext>(Page page) : IPageWithData<StorageFanOutPage<TNext>>
    where TNext : struct, IPageWithData<TNext>
{
    public static StorageFanOutPage<TNext> Wrap(Page page) => new(page);

    private const int ConsumedNibbles = 2;
    private const int LevelDiff = 1;

    private ref PageHeader Header => ref page.Header;

    private ref StorageFanOutPage.Payload Data => ref Unsafe.AsRef<StorageFanOutPage.Payload>(page.Payload);

    public bool TryGet(scoped NibblePath key, IReadOnlyBatchContext batch, out ReadOnlySpan<byte> result)
    {
        var map = new SlottedArray(Data.Data);

        if (map.TryGet(key, out result))
        {
            return true;
        }

        var index = GetIndex(key);

        var addr = Data.Addresses[index];
        if (addr.IsNull)
        {
            result = default;
            return false;
        }

        return TNext.Wrap(batch.GetAt(addr)).TryGet(key.SliceFrom(ConsumedNibbles), batch, out result);
    }

    private static int GetIndex(scoped in NibblePath key) => (key.GetAt(0) << NibblePath.NibbleShift) + key.GetAt(1);

    public Page Set(in NibblePath key, in ReadOnlySpan<byte> data, IBatchContext batch)
    {
        if (Header.BatchId != batch.BatchId)
        {
            // the page is from another batch, meaning, it's readonly. Copy
            var writable = batch.GetWritableCopy(page);
            return new StorageFanOutPage<TNext>(writable).Set(key, data, batch);
        }

        var map = new SlottedArray(Data.Data);

        if (map.TrySet(key, data))
        {
            return page;
        }

        // No space in page, flush down
        foreach (var item in map.EnumerateAll())
        {
            var index = GetIndex(item.Key);
            var sliced = item.Key.SliceFrom(ConsumedNibbles);

            ref var addr = ref Data.Addresses[index];

            Page child;

            if (addr.IsNull)
            {
                child = batch.GetNewPage(out addr, true);
                child.Header.PageType = Header.PageType;
                child.Header.Level = (byte)(page.Header.Level + LevelDiff);
            }
            else
            {
                child = batch.GetAt(addr);
            }

            // set and delete
            addr = batch.GetAddress(TNext.Wrap(child).Set(sliced, item.RawData, batch));
            map.Delete(item);
        }

        // retry
        return Set(key, data, batch);
    }

    public void Report(IReporter reporter, IPageResolver resolver, int level)
    {
        foreach (var bucket in Data.Addresses)
        {
            if (!bucket.IsNull)
            {
                TNext.Wrap(resolver.GetAt(bucket)).Report(reporter, resolver, level + LevelDiff);
            }
        }
    }
    public void Destroy(IBatchContext batch, in NibblePath prefix)
    {
        // Destroy the Id entry about it
        Set(prefix, ReadOnlySpan<byte>.Empty, batch);

        // Destroy the account entry
        // SetAtRoot<FanOutPage>(batch, account, ReadOnlySpan<byte>.Empty, ref Data.StateRoot);

        // Remove the cached
        batch.IdCache.Remove(prefix.UnsafeAsKeccak);
        var index = GetIndex(prefix);
        var addr = Data.Addresses[index];
        if (addr.IsNull)
        {
            // recycleForReuse
        }
        TNext.Wrap(batch.GetAt(addr)).Destroy(batch, prefix.SliceFrom(ConsumedNibbles));
    }
}

static class StorageFanOutPage
{
    [StructLayout(LayoutKind.Explicit, Size = Size)]
    public struct Payload
    {
        private const int Size = Page.PageSize - PageHeader.Size;

        private const int FanOutSize = FanOut * DbAddress.Size;

        private const int DataSize = Size - FanOutSize;

        /// <summary>
        /// The number of buckets to fan out to.
        /// </summary>
        private const int FanOut = 256;

        /// <summary>
        /// The first item of map of frames to allow ref to it.
        /// </summary>
        [FieldOffset(0)] private DbAddress Address;

        public Span<DbAddress> Addresses => MemoryMarshal.CreateSpan(ref Address, FanOut);

        [FieldOffset(FanOutSize)] private byte DataFirst;

        public Span<byte> Data => MemoryMarshal.CreateSpan(ref DataFirst, DataSize);
    }

}

