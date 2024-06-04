using System.Diagnostics;
using System.Diagnostics.Contracts;
using System.Numerics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Paprika.Chain;
using Paprika.Store;

namespace Paprika.Data;

public static class BitMapFilter
{
    public static BitMapFilter<Of1> CreateOf1(Page page) => new(new Of1(page));
    public static BitMapFilter<Of1> CreateOf1(BufferPool pool) => new(new Of1(pool.Rent(true)));

    public static BitMapFilter<Of2> CreateOf2(Page page0, Page page1) => new(new Of2(page0, page1));
    public static BitMapFilter<Of2> CreateOf2(BufferPool pool) => new(new Of2(pool.Rent(true), pool.Rent(true)));

    public static BitMapFilter<OfN> CreateOfN(BufferPool pool, int n)
    {
        var pages = new Page[n];
        for (var i = 0; i < n; i++)
        {
            pages[i] = pool.Rent(true);
        }

        return new BitMapFilter<OfN>(new OfN(pages));
    }


    public interface IAccessor
    {
        public const int BitsPerByteShift = 3;
        public const int BitsPerByte = 1 << BitsPerByteShift;
        public const int BitMask = BitsPerByte - 1;
        public const ulong PageMask = Page.PageSize - 1;

        [Pure]
        unsafe byte* GetBit(ulong hash, out byte bit);

        [Pure]
        public void Return(BufferPool pool);

        public int BucketCount { get; }
    }

    public readonly struct Of1(Page page) : IAccessor
    {
        [Pure]
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public unsafe byte* GetBit(ulong hash, out byte bit)
        {
            bit = (byte)(1 << (int)(hash & IAccessor.BitMask));
            return (byte*)page.Raw.ToPointer() + ((hash >> IAccessor.BitsPerByteShift) & IAccessor.PageMask);
        }

        public void Return(BufferPool pool) => pool.Return(page);
        public int BucketCount => Page.PageSize * IAccessor.BitsPerByte;
    }

    public readonly struct Of2(Page page0, Page page1) : IAccessor
    {
        private const int PageMask = 1;
        private const int PageMaskShift = 1;

        [Pure]
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public unsafe byte* GetBit(ulong hash, out byte bit)
        {
            bit = (byte)(1 << (int)(hash & IAccessor.BitMask));
            var h = hash >> IAccessor.BitsPerByteShift;
            var page = (h & PageMask) == PageMask ? page1 : page0;
            h >>= PageMaskShift;

            return (byte*)page.Raw.ToPointer() + (h & IAccessor.PageMask);
        }

        public void Return(BufferPool pool)
        {
            pool.Return(page0);
            pool.Return(page1);
        }

        public int BucketCount => Page.PageSize * IAccessor.BitsPerByte * 2;
    }

    public readonly struct OfN : IAccessor
    {
        private readonly Page[] _pages;
        private readonly byte _pageMask;
        private readonly byte _pageMaskShift;

        public OfN(Page[] pages)
        {
            _pages = pages;
            Debug.Assert(BitOperations.IsPow2(pages.Length));
            _pageMask = (byte)(pages.Length - 1);
            _pageMaskShift = (byte)BitOperations.Log2((uint)pages.Length);
        }

        [Pure]
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public unsafe byte* GetBit(ulong hash, out byte bit)
        {
            bit = (byte)(1 << (int)(hash & IAccessor.BitMask));
            var h = hash >> IAccessor.BitsPerByteShift;
            var page = Unsafe.Add(ref MemoryMarshal.GetArrayDataReference(_pages), (int)(h & _pageMask));
            h >>= _pageMaskShift;

            return (byte*)page.Raw.ToPointer() + (h & IAccessor.PageMask);
        }

        public void Return(BufferPool pool)
        {
            foreach (var page in _pages)
            {
                pool.Return(page);
            }
        }

        public int BucketCount => Page.PageSize * IAccessor.BitsPerByte * (_pageMask + 1);
    }
}

/// <summary>
/// Represents a simple bitmap based filter.
/// </summary>
public readonly struct BitMapFilter<TAccessor>(TAccessor accessor)
    where TAccessor : struct, BitMapFilter.IAccessor
{
    public unsafe bool this[ulong hash]
    {
        get
        {
            var ptr = accessor.GetBit(hash, out var bit);
            return (*ptr & bit) == bit;
        }
        set
        {
            var ptr = accessor.GetBit(hash, out var bit);
            if (value)
            {
                *ptr = (byte)(*ptr | bit);
            }
            else
            {
                *ptr = (byte)(*ptr & ~bit);
            }
        }
    }

    public int BucketCount => accessor.BucketCount;

    public void Return(BufferPool pool) => accessor.Return(pool);
}