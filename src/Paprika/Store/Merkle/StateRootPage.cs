using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Paprika.Data;
using Paprika.Merkle;

namespace Paprika.Store.Merkle;

[method: DebuggerStepThrough]
public readonly unsafe struct StateRootPage(Page page) : IPageWithData<StateRootPage>
{
    public static StateRootPage Wrap(Page page) => Unsafe.As<Page, StateRootPage>(ref page);

    private const int ConsumedNibbles = ComputeMerkleBehavior.SkipRlpMemoizationForTopLevelsCount;
    private const int BucketCount = 16 * 16;

    private static int GetIndex(scoped in NibblePath key) =>
        (key.GetAt(0) << NibblePath.NibbleShift) + key.GetAt(1);

    private ref PageHeader Header => ref page.Header;

    private ref Payload Data => ref Unsafe.AsRef<Payload>(page.Payload);

    public Page Set(in NibblePath key, in ReadOnlySpan<byte> data, IBatchContext batch)
    {
        if (Header.BatchId != batch.BatchId)
        {
            // the page is from another batch, meaning, it's readonly. Copy
            var writable = batch.GetWritableCopy(page);
            return new StateRootPage(writable).Set(key, data, batch);
        }

        if (key.Length < ConsumedNibbles)
        {
            var map = new SlottedArray(Data.DataSpan);
            var isDelete = data.IsEmpty;
            if (isDelete)
            {
                map.Delete(key);
            }
            else
            {
                var succeeded = map.TrySet(key, data);
                Debug.Assert(succeeded);
            }

            return page;
        }

        var index = GetIndex(key);
        var sliced = key.SliceFrom(ConsumedNibbles);
        ref var addr = ref Data.Buckets[index];

        if (addr.IsNull)
        {
            var child = batch.GetNewPage(out addr, true);
            child.Header.Level = ConsumedNibbles;
            child.Header.PageType = PageType.MerkleFanOut;
            new FanOutPage(child).Set(sliced, data, batch);
        }
        else
        {
            addr = batch.GetAddress(new FanOutPage(batch.GetAt(addr)).Set(sliced, data, batch));
        }

        return page;
    }

    /// <summary>
    /// Represents the data of this data page. This type of payload stores data in 16 nibble-addressable buckets.
    /// These buckets is used to store up to <see cref="DataSize"/> entries before flushing them down as other pages
    /// like page split.
    /// </summary>
    [StructLayout(LayoutKind.Explicit, Size = Size)]
    private struct Payload
    {
        private const int Size = Page.PageSize - PageHeader.Size;
        private const int BucketSize = BucketCount * DbAddress.Size;

        /// <summary>
        /// The size of the raw byte data held in this page. Must be long aligned.
        /// </summary>
        private const int DataSize = Size - BucketSize;

        private const int DataOffset = Size - DataSize;

        /// <summary>
        /// The first field of buckets.
        /// </summary>
        [FieldOffset(0)] private DbAddress Bucket;

        public Span<DbAddress> Buckets => MemoryMarshal.CreateSpan(ref Bucket, BucketCount);

        /// <summary>
        /// The first item of map of frames to allow ref to it.
        /// </summary>
        [FieldOffset(DataOffset)] private byte DataStart;

        /// <summary>
        /// Writable area.
        /// </summary>
        public Span<byte> DataSpan => MemoryMarshal.CreateSpan(ref DataStart, DataSize);
    }

    public bool TryGet(IReadOnlyBatchContext batch, scoped in NibblePath key, out ReadOnlySpan<byte> result)
    {
        if (key.Length < ConsumedNibbles)
        {
            return new SlottedArray(Data.DataSpan).TryGet(key, out result);
        }

        var index = GetIndex(key);
        var addr = Data.Buckets[index];
        if (addr.IsNull)
        {
            result = default;
            return false;
        }

        return new FanOutPage(batch.GetAt(addr)).TryGet(batch, key.SliceFrom(ConsumedNibbles), out result);
    }

    public void Report(IReporter reporter, IPageResolver resolver, int pageLevel, int trimmedNibbles)
    {
        foreach (var bucket in Data.Buckets)
        {
            if (bucket.IsNull)
                continue;

            var consumedNibbles = trimmedNibbles + ConsumedNibbles;
            var lvl = pageLevel + 1;

            var child = resolver.GetAt(bucket);

            new FanOutPage(child).Report(reporter, resolver, lvl, consumedNibbles);
        }

        var slotted = new SlottedArray(Data.DataSpan);
        reporter.ReportDataUsage(Header.PageType, pageLevel, trimmedNibbles, slotted);
    }

    public void Accept(IPageVisitor visitor, IPageResolver resolver, DbAddress addr)
    {
        using (visitor.On(this, addr))
        {
            foreach (var bucket in Data.Buckets)
            {
                if (bucket.IsNull)
                {
                    continue;
                }

                var child = resolver.GetAt(bucket);
                switch (child.Header.PageType)
                {
                    case PageType.MerkleFanOut:
                        new FanOutPage(child).Accept(visitor, resolver, bucket);
                        break;
                    case PageType.MerkleLeaf:
                        new LeafPage(child).Accept(visitor, resolver, bucket);
                        break;
                    default:
                        throw new Exception($"Type {child.Header.PageType} not handled");
                }
            }
        }
    }
}