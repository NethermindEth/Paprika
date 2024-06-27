using System.Diagnostics;
using System.Runtime.InteropServices;
using Paprika.Data;

namespace Paprika.Store;

public static class FanOutList
{
    public const int Size = FanOut * DbAddress.Size;

    /// <summary>
    /// The number of buckets to fan out to.
    /// </summary>
    public const int FanOut = 256;
}

/// <summary>
/// Provides a convenient data structure for <see cref="RootPage"/>, to preserve a fan out of
/// <see cref="FanOut"/> pages underneath. 
/// </summary>
/// <remarks>
/// The main idea is to limit the depth of the tree by 1 or two and use the space in <see cref="RootPage"/> more.
/// </remarks>
public readonly ref struct FanOutList<TPage, TPageType>(Span<DbAddress> addresses)
    where TPage : struct, IPageWithData<TPage>
    where TPageType : IPageTypeProvider
{
    private readonly Span<DbAddress> _addresses = addresses;
    private const int ConsumedNibbles = 2;

    public bool TryGet(IReadOnlyBatchContext batch, scoped in NibblePath key, out ReadOnlySpan<byte> result)
    {
        var index = GetIndex(key);

        var addr = _addresses[index];
        if (addr.IsNull)
        {
            result = default;
            return false;
        }

        return TPage.Wrap(batch.GetAt(addr)).TryGet(batch, key.SliceFrom(ConsumedNibbles), out result);
    }

    private static int GetIndex(scoped in NibblePath key) => (key.GetAt(0) << NibblePath.NibbleShift) + key.GetAt(1);

    public void Set(in NibblePath key, in ReadOnlySpan<byte> data, IBatchContext batch)
    {
        var index = GetIndex(key);
        var sliced = key.SliceFrom(ConsumedNibbles);

        ref var addr = ref _addresses[index];

        if (addr.IsNull)
        {
            var newPage = batch.GetNewPage(out addr, true);
            newPage.Header.PageType = TPageType.Type;
            newPage.Header.Level = 0;

            TPage.Wrap(newPage).Set(sliced, data, batch);
            return;
        }

        // The page exists, update
        var updated = TPage.Wrap(batch.GetAt(addr)).Set(sliced, data, batch);
        addr = batch.GetAddress(updated);
    }

    public void Report(IReporter reporter, IPageResolver resolver, int level, int trimmedNibbles)
    {
        var consumedNibbles = trimmedNibbles + ConsumedNibbles;

        foreach (var bucket in _addresses)
        {
            if (!bucket.IsNull)
            {
                TPage.Wrap(resolver.GetAt(bucket)).Report(reporter, resolver, level + 1, consumedNibbles);
            }
        }
    }

    public void Accept(IPageVisitor visitor, IPageResolver resolver)
    {
        foreach (var bucket in _addresses)
        {
            if (!bucket.IsNull)
            {
                TPage.Wrap(resolver.GetAt(bucket)).Accept(visitor, resolver, bucket);
            }
        }
    }
}

/// <summary>
/// A densely packed fan out of 4 nibbles. It uses raw pages to encode addresses that it points to.
/// It manages COW behavior on its own.
/// </summary>
/// <typeparam name="TPage"></typeparam>
/// <typeparam name="TPageType"></typeparam>
[StructLayout(LayoutKind.Explicit, Size = Size)]
public struct FanOutOf4<TPage, TPageType>
    where TPage : struct, IPageWithData<TPage>
    where TPageType : IPageTypeProvider
{
    public const int Size = PageCount * DbAddress.Size + CowBitVectorSize;

    private const int DbAddressesPerPage = Page.PageSize / DbAddress.Size;
    private const int PageCount = 64;
    private const int ExtractedNibbles = 4;
    private const int CowBitVectorSize = sizeof(ulong);

    [FieldOffset(0)] private long CowBitVector;

    [FieldOffset(CowBitVectorSize)] private DbAddress Start;

    private Span<DbAddress> Addresses => MemoryMarshal.CreateSpan(ref Start, PageCount);

    public void ClearCowVector()
    {
        CowBitVector = 0;
    }

    public void Set(in NibblePath key, in ReadOnlySpan<byte> data, IBatchContext batch)
    {
        var e = Extract(key, batch, batch);

        if (e.Address.IsNull)
        {
            var newPage = batch.GetNewPage(out e.Address, true);
            newPage.Header.PageType = TPageType.Type;
            newPage.Header.Level = 0;

            TPage.Wrap(newPage).Set(e.Key, data, batch);
            return;
        }

        // The page exists, update
        var updated = TPage.Wrap(batch.GetAt(e.Address)).Set(e.Key, data, batch);
        e.Address = batch.GetAddress(updated);
    }

    public bool TryGet(IReadOnlyBatchContext batch, scoped in NibblePath key, out ReadOnlySpan<byte> result)
    {
        var e = Extract(key, batch);

        if (e.Address.IsNull)
        {
            result = default;
            return false;
        }

        var k = e.Key;
        return TPage.Wrap(batch.GetAt(e.Address)).TryGet(batch, k, out result);
    }
    
    public void Accept(IPageVisitor visitor, IPageResolver resolver)
    {
        foreach (var addr in Addresses)
        {
            if (!addr.IsNull)
            {
                TPage.Wrap(resolver.GetAt(addr)).Accept(visitor, resolver, addr);
            }
        }
    }

    private Result Extract(scoped in NibblePath key, IReadOnlyBatchContext batch, IBatchContext? cowContext = null)
    {
        Debug.Assert(key.Length > ExtractedNibbles);

        const int shift = NibblePath.NibbleShift;

        var slot =
            key.GetAt(0) +
            (key.GetAt(1) << shift) +
            (key.GetAt(2) << (2 * shift)) +
            (key.GetAt(3) << (3 * shift));

        Debug.Assert(slot < DbAddressesPerPage * PageCount);

        var (index, pageNo) = Math.DivRem(slot, PageCount);
        ref var addr = ref Addresses[pageNo];
        var page = batch.GetAt(addr);
        
        if (cowContext != null)
        {
            // COW context is not null, it means that the page will be written to
            var flag = 1L << pageNo;
            
            if ((CowBitVector & flag) == 0)
            {
                CowBitVector |= flag;
                page = cowContext.GetWritableCopy(page);
                addr = cowContext.GetAddress(page);
            }
        }
        
        var children = MemoryMarshal.Cast<byte, DbAddress>(page.Span);
        return new Result(key.SliceFrom(ExtractedNibbles), ref children[index]);
    }

    private readonly ref struct Result
    {
        public readonly NibblePath Key;
        public readonly ref DbAddress Address;

        public Result(NibblePath key, ref DbAddress address)
        {
            Key = key;
            Address = ref address;
        }
    }
}