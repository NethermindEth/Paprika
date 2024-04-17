using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Paprika.Crypto;
using Paprika.Data;

namespace Paprika.Store;

/// <summary>
/// Represents a set of root pages for storage
/// </summary>
/// <param name="page"></param>
public readonly unsafe struct StorageRootPage(Page page)
{
    public ref PageHeader Header => ref page.Header;
    public ref Payload Data => ref Unsafe.AsRef<Payload>(page.Payload);
    public bool HasEmptySlot => Data.Buckets.IndexOf(Keccak.Zero) != NotFound;

    public ReadOnlySpan<Keccak> Keys => Data.Buckets;

    private const int NotFound = -1;

    public bool TryGet(scoped in Key key, IReadOnlyBatchContext batch, out ReadOnlySpan<byte> result)
    {
        var index = FindIndex(key);
        if (index == NotFound)
        {
            result = default;
            return false;
        }

        return Data.Slots[index].TryGet(in key.StoragePath, batch, out result);
    }

    private int FindIndex(in Key key) => Data.Buckets.IndexOf(key.Path.UnsafeAsKeccak);

    public Page Set(scoped in Key key, in ReadOnlySpan<byte> data, IBatchContext batch)
    {
        if (Header.BatchId != batch.BatchId)
        {
            return new StorageRootPage(batch.GetWritableCopy(page)).Set(key, data, batch);
        }

        var index = FindIndex(key);
        if (index == NotFound)
        {
            index = Data.Buckets.IndexOf(Keccak.Zero);

            Debug.Assert(index != NotFound);
            Data.Buckets[index] = key.Path.UnsafeAsKeccak;
        }

        Data.Slots[index].Set(key.StoragePath, data, batch);

        return page;
    }

    public void Report(IReporter reporter, IPageResolver resolver, int pageLevel, int trimmedNibbles)
    {
        throw new NotImplementedException();
    }

    public void Accept(IPageVisitor visitor, IPageResolver resolver, DbAddress addr)
    {
        throw new NotImplementedException();
    }

    [StructLayout(LayoutKind.Explicit, Size = Size)]
    public struct Payload
    {
        private const int Size = Page.PageSize - PageHeader.Size;
        private const int BucketCount = 4;
        private const int BucketsSize = BucketCount * Keccak.Size;

        /// <summary>
        /// Address buckets.
        /// </summary>
        [FieldOffset(0)] private Keccak Bucket;
        public Span<Keccak> Buckets => MemoryMarshal.CreateSpan(ref Bucket, BucketCount);

        /// <summary>
        /// The first item of map of frames to allow ref to it.
        /// </summary>
        [FieldOffset(BucketsSize)] private Slot SlotStart;

        /// <summary>
        /// Slots.
        /// </summary>
        public Span<Slot> Slots => MemoryMarshal.CreateSpan(ref SlotStart, BucketCount);

        [StructLayout(LayoutKind.Explicit, Size = SlotSize)]
        public struct Slot
        {
            // start is long aligned for SlottedArray
            private const int SlotSize = Page.PageSize / BucketCount - Keccak.Size;

            // Should be enough to keep the full branch of the Merkle
            private const int DataLength = SlotSize - DbAddress.Size;

            [FieldOffset(0)]
            private byte DataStart;

            private SlottedArray Data => new(MemoryMarshal.CreateSpan(ref DataStart, DataLength));

            [FieldOffset(DataLength)]
            public DbAddress Tree;

            public bool TryGet(scoped in NibblePath path, IReadOnlyBatchContext batch, out ReadOnlySpan<byte> result)
            {
                if (Data.TryGet(path, out result))
                {
                    return true;
                }

                if (Tree.IsNull)
                {
                    return false;
                }

                return batch.GetAt(Tree).GetPageWithData(batch, path, out result);
            }

            public void Set(in NibblePath path, in ReadOnlySpan<byte> data, IBatchContext batch)
            {
                if (Data.TrySet(path, data))
                    return;

                if (Tree.IsNull)
                {
                    batch.GetNewLeaf(0, out Tree);
                }

                foreach (var item in Data.EnumerateAll())
                {
                    var page = batch.GetAt(Tree);
                    var updated = page.SetPageWithData(item.Key, item.RawData, batch);

                    if (page.Equals(updated) == false)
                    {
                        Tree = batch.GetAddress(updated);
                    }

                    Data.Delete(item);
                }

                Set(in path, in data, batch);
            }
        }
    }
}