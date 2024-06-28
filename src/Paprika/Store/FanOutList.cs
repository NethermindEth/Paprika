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
            ref var addr = ref GetBucket(key, out var index);

            // Ensure the first page is properly set
            Page page;
            if (addr.IsNull)
            {
                page = batch.GetNewPage(out addr, true);
                page.Header.PageType = PageType.Standard;
                page.Header.Level = 0;
            }
            else
            {
                page = batch.EnsureWritableCopy(ref addr);
            }

            ref var descendant = ref GetDescendantAddress(page, index);
            // The page exists, update
            var updated = TPage.Wrap(batch.GetAt(descendant)).Set(key.SliceFrom(ConsumedNibbles), data, batch);
            descendant = batch.GetAddress(updated);
        }

        public bool TryGet(IReadOnlyBatchContext batch, scoped in NibblePath key, out ReadOnlySpan<byte> result)
        {
            ref var addr = ref GetBucket(key, out var index);

            if (addr.IsNull)
            {
                result = default;
                return false;
            }

            var descendant = GetDescendantAddress(batch.GetAt(addr), index);
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

        private ref DbAddress GetBucket(in NibblePath key, out int index)
        {
            Debug.Assert(key.Length > ConsumedNibbles);

            const int shift = NibblePath.NibbleShift;

            var bucket =
                key.GetAt(0) +
                (key.GetAt(1) << shift) +
                (key.GetAt(2) << (2 * shift)) +
                (key.GetAt(3) << (3 * shift));

            Debug.Assert(bucket < DbAddressesPerPage * PageCount);
            (index, var pageNo) = Math.DivRem(bucket, PageCount);
            return ref _data.Addresses[pageNo];
        }

        private static ref DbAddress GetDescendantAddress(Page page, int index)
        {
            var children = MemoryMarshal.Cast<byte, DbAddress>(page.Span);
            return ref children[index];
        }
    }
}