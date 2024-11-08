using System.Diagnostics;
using System.Diagnostics.Contracts;
using System.Numerics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Runtime.Intrinsics.X86;
using Paprika.Chain;
using Paprika.Store;
using Paprika.Utils;

namespace Paprika.Data;

public static class BitMapFilter
{
    private const int BitsPerByte = 8;
    private const int SlotsPerPage = Page.PageSize / sizeof(int);
    private const int SlotsPerPageMask = SlotsPerPage - 1;

    public static BitMapFilter<Of1> CreateOf1(BufferPool pool) => new(new Of1(pool.Rent(true)));

    public static BitMapFilter<Of2> CreateOf2(BufferPool pool) => new(new Of2(pool.Rent(true), pool.Rent(true)));

    public static BitMapFilter<OfN<TSize>> CreateOfN<TSize>(BufferPool pool)
        where TSize : IOfNSize
    {
        var pages = new Page[TSize.Count];
        for (var i = 0; i < TSize.Count; i++)
        {
            pages[i] = pool.Rent(true);
        }

        return new BitMapFilter<OfN<TSize>>(new OfN<TSize>(pages));
    }

    public interface IAccessor<TAccessor>
        where TAccessor : struct, IAccessor<TAccessor>
    {
        [Pure]
        ref int GetSlot(uint hash);

        [Pure]
        void Clear();

        [Pure]
        void Return(BufferPool pool);

        int BucketCount { get; }

        [Pure]
        void OrWith(in TAccessor other);

        [Pure]
        void OrWith(TAccessor[] others)
        {
            foreach (var other in others)
            {
                OrWith(other);
            }
        }
    }

    public readonly struct Of1(Page page) : IAccessor<Of1>
    {
        private readonly Page _page = page;

        public unsafe ref int GetSlot(uint hash) =>
            ref Unsafe.Add(ref Unsafe.AsRef<int>(_page.Raw.ToPointer()), hash & SlotsPerPageMask);

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

        public unsafe ref int GetSlot(uint hash)
        {
            const int pageMask = 1;
            const int pageMaskShift = 1;

            var page = (hash & pageMask) != pageMask ? _page0 : _page1;
            var index = (hash >> pageMaskShift) & SlotsPerPageMask;

            return ref Unsafe.Add(ref Unsafe.AsRef<int>(page.Raw.ToPointer()), index);
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

    public interface IOfNSize
    {
        public static abstract int Count { get; }
    }

    public struct OfNSize128 : IOfNSize
    {
        public static int Count => 128;
    }

    public readonly struct OfN<TSize> : IAccessor<OfN<TSize>>
        where TSize : IOfNSize
    {
        private readonly Page[] _pages;

        private static int PageMask => TSize.Count - 1;
        private static int PageMaskShift => BitOperations.Log2((uint)TSize.Count);

        public OfN(Page[] pages)
        {
            _pages = pages;
            Debug.Assert(pages.Length == TSize.Count);
        }

        public unsafe ref int GetSlot(uint hash)
        {
            var page = Unsafe.Add(ref MemoryMarshal.GetArrayDataReference(_pages), (int)(hash & PageMask));
            var index = (hash >> PageMaskShift) & SlotsPerPageMask;

            return ref Unsafe.Add(ref Unsafe.AsRef<int>(page.Raw.ToPointer()), index);
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

        public void OrWith(in OfN<TSize> other)
        {
            var count = PageCount;

            Debug.Assert(PageCount == count);

            ref var a = ref MemoryMarshal.GetArrayDataReference(_pages);
            ref var b = ref MemoryMarshal.GetArrayDataReference(other._pages);

            for (var i = 0; i < count; i++)
            {
                Unsafe.Add(ref a, i).OrWith(Unsafe.Add(ref b, i));
            }
        }

        [Pure]
        public void OrWith(OfN<TSize>[] others)
        {
            State state = new State(others, _pages);

            WorkProcessor.For(0, PageCount, state, static (i, s) =>
            {
                var page = s.Pages[i];
                var length = s.Others.Length;

                for (var j = 0; j < length - 1; j++)
                {
                    if (Sse.IsSupported)
                    {
                        // prefetch next
                        unsafe
                        {
                            Sse.Prefetch2(s.Others[j + 1]._pages[i].Payload);
                        }
                    }

                    page.OrWith(s.Others[j]._pages[i]);
                }

                page.OrWith(s.Others[length - 1]._pages[i]);

                return s;
            });
        }

        private readonly record struct State(OfN<TSize>[] Others, Page[] Pages);

        public int BucketCount => Page.PageSize * BitsPerByte * PageCount;

        private static int PageCount => TSize.Count;
    }
}

/// <summary>
/// Represents a simple bitmap based filter for <see cref="ulong"/> hashes.
/// </summary>
public readonly struct BitMapFilter<TAccessor>
    where TAccessor : struct, BitMapFilter.IAccessor<TAccessor>
{
    private readonly TAccessor _accessor;
    private const int BitsPerIntShift = 5;
    private const int BitsPerInt = 1 << BitsPerIntShift;
    private const int BitMask = BitsPerInt - 1;

    /// <summary>
    /// Represents a simple bitmap based filter.
    /// </summary>
    public BitMapFilter(TAccessor accessor)
    {
        _accessor = accessor;
    }

    public void Add(ulong hash)
    {
        ref var slot = ref GetSlot(hash, out var mask);
        slot |= mask;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private ref int GetSlot(ulong hash, out int mask)
    {
        var mixed = Mix(hash);
        mask = GetBitMask(mixed);
        return ref _accessor.GetSlot(mixed >> BitsPerIntShift);
    }

    /// <summary>
    /// Adds the hash atomically.
    /// </summary>
    /// <param name="hash"></param>
    /// <returns>Whether it was added after this operation.</returns>
    public bool AddAtomic(ulong hash)
    {
        ref var slot = ref GetSlot(hash, out var mask);

        // was it 0 before? Yes, return true
        return (Interlocked.Or(ref slot, mask) & mask) == 0;
    }

    public bool MayContain(ulong hash)
    {
        ref var slot = ref GetSlot(hash, out var mask);
        return (slot & mask) == mask;
    }

    public bool MayContainVolatile(ulong hash)
    {
        ref var slot = ref GetSlot(hash, out var mask);
        return (Volatile.Read(ref slot) & mask) == mask;
    }

    /// <summary>
    /// Checks whether the filter may contain any of the hashes.
    /// </summary>
    [SkipLocalsInit]
    public bool MayContainAny(ulong hash0, ulong hash1)
    {
        var mixed0 = Mix(hash0);
        var slot0 = _accessor.GetSlot(mixed0 >> BitsPerIntShift);
        var mixed1 = Mix(hash1);
        var slot1 = _accessor.GetSlot(mixed1 >> BitsPerIntShift);

        return ((slot0 & GetBitMask(mixed0)) | (slot1 & GetBitMask(mixed1))) != 0;
    }

    public void Clear() => _accessor.Clear();

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static int GetBitMask(uint mixed) => 1 << (int)(mixed & BitMask);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static uint Mix(ulong hash) => (uint)((hash >> 32) ^ hash);

    /// <summary>
    /// Applies or operation with <paramref name="other"/> bitmap filter and stores it in this one.
    /// </summary>
    /// <param name="other"></param>
    public void OrWith(in BitMapFilter<TAccessor> other)
    {
        _accessor.OrWith(other._accessor);
    }

    /// <summary>
    /// Applies OR operation with all the <paramref name="others"/> filters and stores it in this one.
    /// </summary>
    /// <remarks>
    /// A bulk version of <see cref="OrWith(in Paprika.Data.BitMapFilter{TAccessor})"/>.
    /// </remarks>
    public void OrWith(BitMapFilter<TAccessor>[] others)
    {
        var copy = new TAccessor[others.Length];
        for (int i = 0; i < others.Length; i++)
        {
            copy[i] = others[i]._accessor;
        }

        _accessor.OrWith(copy);
    }

    public int BucketCount => _accessor.BucketCount;

    public void Return(BufferPool pool) => _accessor.Return(pool);
}