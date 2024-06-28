using System.Diagnostics;
using System.Runtime.InteropServices;
using Paprika.Data;

namespace Paprika.Store;

[StructLayout(LayoutKind.Explicit, Size = Size)]
public struct FanOutList
{
    public const int Size = CowBitVectorSize + PageCount * DbAddress.Size;
    private const int DbAddressesPerPage = Page.PageSize / DbAddress.Size;
    private const int PageCount = 64;
    private const int ConsumedNibbles = 4;
    private const int CowBitVectorSize = sizeof(ulong);

    [FieldOffset(0)] private long CowBitVector;

    [FieldOffset(CowBitVectorSize)] private DbAddress Start;
    private Span<DbAddress> Addresses => MemoryMarshal.CreateSpan(ref Start, PageCount);

    public readonly ref struct Of<TPage, TPageType> where TPage : struct, IPageWithData<TPage>
        where TPageType : IPageTypeProvider
    {
        private readonly ref FanOutList _data;

        public Of(ref FanOutList data)
        {
            _data = ref data;
        }

        public void ClearCowVector()
        {
            _data.CowBitVector = 0;
        }

        public void Set(in NibblePath key, in ReadOnlySpan<byte> data, IBatchContext batch)
        {
            ref var addr = ref GetBucket(key, out var index, out var bucket);
            var cowFlag = 1L << bucket;

            // The page that contains the buckets requires manual management as it has no header.
            Page page;
            if (addr.IsNull)
            {
                // The page did not exist before.
                // Get a new but remember that the manual clearing is required to destroy assigned metadata.
                page = batch.GetNewPage(out addr, false);
                page.Clear();
            }
            else
            {
                if ((_data.CowBitVector & cowFlag) != cowFlag)
                {
                    // This page have not been COWed during this batch.
                    // This must be done in a manual way as the header is overwritten.
                    var prev = batch.GetAt(addr);
                    page = batch.GetNewPage(out addr, false);
                    prev.CopyTo(page);
                    batch.RegisterForFutureReuse(prev);

                    // Mark the flag so that the next one does not COW again.
                    _data.CowBitVector |= cowFlag;
                }
                else
                {
                    // This page has been COWed already, just retrieve.
                    page = batch.GetAt(addr);
                }
            }

            ref var descendant = ref GetDescendantAddress(page, index);
            if (descendant.IsNull)
            {
                var descendantPage = batch.GetNewPage(out descendant, true);
                descendantPage.Header.PageType = TPageType.Type;
            }

            // The page exists, update
            var updated = TPage.Wrap(batch.GetAt(descendant)).Set(key.SliceFrom(ConsumedNibbles), data, batch);
            descendant = batch.GetAddress(updated);
        }

        public bool TryGet(IReadOnlyBatchContext batch, scoped in NibblePath key, out ReadOnlySpan<byte> result)
        {
            ref var addr = ref GetBucket(key, out var index, out _);

            if (addr.IsNull)
            {
                result = default;
                return false;
            }

            var descendant = GetDescendantAddress(batch.GetAt(addr), index);
            if (descendant.IsNull)
            {
                result = default;
                return false;
            }

            return TPage.Wrap(batch.GetAt(descendant)).TryGet(batch, key.SliceFrom(ConsumedNibbles), out result);
        }

        public void Accept(IPageVisitor visitor, IPageResolver resolver)
        {
            foreach (var addr in _data.Addresses)
            {
                if (!addr.IsNull)
                {
                    TPage.Wrap(resolver.GetAt(addr)).Accept(visitor, resolver, addr);
                }
            }
        }

        private ref DbAddress GetBucket(in NibblePath key, out int index, out int pageNo)
        {
            Debug.Assert(key.Length > ConsumedNibbles);

            const int shift = NibblePath.NibbleShift;

            var bucket =
                key.GetAt(0) +
                (key.GetAt(1) << shift) +
                (key.GetAt(2) << (2 * shift)) +
                (key.GetAt(3) << (3 * shift));

            Debug.Assert(bucket < DbAddressesPerPage * PageCount);
            (index, pageNo) = Math.DivRem(bucket, PageCount);
            return ref _data.Addresses[pageNo];
        }

        private static ref DbAddress GetDescendantAddress(Page page, int index)
        {
            var children = MemoryMarshal.Cast<byte, DbAddress>(page.Span);
            return ref children[index];
        }
    }
}