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
    private const int BitsPerByteShift = 3;
    private const int BitsPerByte = 1 << BitsPerByteShift;
    private const int BitMask = BitsPerByte - 1;
    private const ulong PageMask = Page.PageSize - 1;

    public static BitMapFilter<Of1> CreateOf1(BufferPool pool) => new(new Of1(pool.Rent(true)));

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

    public interface IAccessor<TAccessor>
        where TAccessor : struct, IAccessor<TAccessor>
    {
        [Pure]
        unsafe byte* GetBit(ulong hash, out byte bit);

        [Pure]
        void Clear();

        [Pure]
        void Return(BufferPool pool);

        int BucketCount { get; }

        void OrWith(in TAccessor other);
    }

    public readonly struct Of1 : IAccessor<Of1>
    {
        private readonly Page _page;

        public Of1(Page page)
        {
            _page = page;
        }

        [Pure]
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public unsafe byte* GetBit(ulong hash, out byte bit)
        {
            bit = (byte)(1 << (int)(hash & BitMask));
            return (byte*)_page.Raw.ToPointer() + ((hash >> BitsPerByteShift) & PageMask);
        }

        public void Clear() => _page.Clear();

        public void Return(BufferPool pool) => pool.Return(_page);
        public int BucketCount => Page.PageSize * BitsPerByte;

        public void OrWith(in Of1 other) => _page.OrWith(other._page);
    }

    public readonly struct Of2 : IAccessor<Of2>
    {
        private readonly Page _page0;
        private readonly Page _page1;

        public Of2(Page page0, Page page1)
        {
            _page0 = page0;
            _page1 = page1;
        }

        [Pure]
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public unsafe byte* GetBit(ulong hash, out byte bit)
        {
            const int pageMask = 1;
            const int pageMaskShift = 1;

            bit = (byte)(1 << (int)(hash & BitMask));
            var h = hash >> BitsPerByteShift;
            var page = (h & pageMask) == pageMask ? _page1 : _page0;
            h >>= pageMaskShift;

            return (byte*)page.Raw.ToPointer() + (h & PageMask);
        }

        public void Clear()
        {
            _page0.Clear();
            _page1.Clear();
        }

        public void Return(BufferPool pool)
        {
            pool.Return(_page0);
            pool.Return(_page1);
        }

        public int BucketCount => Page.PageSize * BitsPerByte * 2;

        public void OrWith(in Of2 other)
        {
            _page0.OrWith(other._page0);
            _page1.OrWith(other._page1);
        }
    }

    public readonly struct OfN : IAccessor<OfN>
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
            bit = (byte)(1 << (int)(hash & BitMask));
            var h = hash >> BitsPerByteShift;
            var page = Unsafe.Add(ref MemoryMarshal.GetArrayDataReference(_pages), (int)(h & _pageMask));
            h >>= _pageMaskShift;

            return (byte*)page.Raw.ToPointer() + (h & PageMask);
        }

        public void Clear()
        {
            foreach (var page in _pages)
            {
                page.Clear();
            }
        }

        public void Return(BufferPool pool)
        {
            foreach (var page in _pages)
            {
                pool.Return(page);
            }
        }

        public void OrWith(in OfN other)
        {
            var count = PageCount;

            Debug.Assert(other.PageCount == count);

            ref var a = ref MemoryMarshal.GetArrayDataReference(_pages);
            ref var b = ref MemoryMarshal.GetArrayDataReference(other._pages);

            for (var i = 0; i < count; i++)
            {
                Unsafe.Add(ref a, i).OrWith(Unsafe.Add(ref b, i));
            }
        }

        public int BucketCount => Page.PageSize * BitsPerByte * PageCount;

        private int PageCount => _pageMask + 1;
    }
}

/// <summary>
/// Represents a simple bitmap based filter.
/// </summary>
public readonly struct BitMapFilter<TAccessor>
    where TAccessor : struct, BitMapFilter.IAccessor<TAccessor>
{
    private readonly TAccessor _accessor;

    /// <summary>
    /// Represents a simple bitmap based filter.
    /// </summary>
    public BitMapFilter(TAccessor accessor)
    {
        _accessor = accessor;
    }

    public void Add(ulong hash) => this[hash] = true;

    public bool MayContain(ulong hash) => this[hash];

    public void Clear() => _accessor.Clear();

    public unsafe bool this[ulong hash]
    {
        get
        {
            var ptr = _accessor.GetBit(hash, out var bit);
            return (*ptr & bit) == bit;
        }
        set
        {
            var ptr = _accessor.GetBit(hash, out var bit);
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

    /// <summary>
    /// Applies or operation with <paramref name="other"/> bitmap filter and stores it in this one.
    /// </summary>
    /// <param name="other"></param>
    public void OrWith(in BitMapFilter<TAccessor> other)
    {
        _accessor.OrWith(other._accessor);
    }

    public int BucketCount => _accessor.BucketCount;

    public void Return(BufferPool pool) => _accessor.Return(pool);
}