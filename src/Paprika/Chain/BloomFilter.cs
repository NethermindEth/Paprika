using System.Collections.Specialized;
using System.Runtime.CompilerServices;
using Paprika.Store;

namespace Paprika.Chain;

/// <summary>
/// Wraps over a <see cref="Page"/> and provides a simple <see cref="BitVector32"/> alternative.
/// </summary>
public readonly struct BloomFilter
{
    private readonly Page _page;
    private const int BitPerByte = 8;
    private const int BitPerLong = 64;
    private const int Mask = Page.PageSize * BitPerByte - 1;

    public BloomFilter(Page page) => _page = page;

    public void Set(int hash)
    {
        ref var value = ref GetRef(hash, out var bit);
        value |= 1L << bit;
    }

    public bool IsSet(int hash)
    {
        ref var value = ref GetRef(hash, out var bit);
        var mask = 1L << bit;
        return (value & mask) == mask;
    }

    private unsafe ref long GetRef(int hash, out int bit)
    {
        var masked = hash & Mask;
        bit = Math.DivRem(masked, BitPerLong, out var longOffset);

        // the memory is page aligned, safe to get by ref
        return ref Unsafe.AsRef<long>((byte*)_page.Raw.ToPointer() + longOffset * sizeof(long));
    }
}
