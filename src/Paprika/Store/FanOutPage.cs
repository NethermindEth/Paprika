using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Paprika.Data;

namespace Paprika.Store;

/// <summary>
/// The counterpart to <see cref="FanOutListOf256{TPage,TPageType}"/> that the list navigates to.
/// It allows to further a high fanout
/// </summary>
[method: DebuggerStepThrough]
public readonly unsafe struct FanOutPage<TPage, TPageType>(Page page) : IPageWithData<FanOutPage<TPage, TPageType>>
    where TPage : struct, IPageWithData<TPage>
    where TPageType : IPageTypeProvider
{
    public static FanOutPage<TPage, TPageType> Wrap(Page page) =>
        Unsafe.As<Page, FanOutPage<TPage, TPageType>>(ref page);

    /// <summary>
    /// See <see cref="FanOutPage.Payload.Addresses"/> size
    /// </summary>
    private const int ConsumedNibbles = 2;

    private ref PageHeader Header => ref page.Header;

    private ref FanOutPage.Payload Data => ref Unsafe.AsRef<FanOutPage.Payload>(page.Payload);

    public bool TryGet(IReadOnlyBatchContext batch, scoped in NibblePath key, out ReadOnlySpan<byte> result)
    {
        batch.AssertRead(Header);

        var index = GetIndex(key);

        var addr = Data.Addresses[index];
        if (addr.IsNull)
        {
            result = default;
            return false;
        }

        return TPage.Wrap(batch.GetAt(addr)).TryGet(batch, key.SliceFrom(ConsumedNibbles), out result);
    }

    private static int GetIndex(scoped in NibblePath key) => (key.GetAt(0) << NibblePath.NibbleShift) + key.GetAt(1);

    public Page Set(in NibblePath key, in ReadOnlySpan<byte> data, IBatchContext batch)
    {
        if (Header.BatchId != batch.BatchId)
        {
            // the page is from another batch, meaning, it's readonly. Copy
            var writable = batch.GetWritableCopy(page);
            return new FanOutPage<TPage, TPageType>(writable).Set(key, data, batch);
        }

        var index = GetIndex(key);
        var sliced = key.SliceFrom(ConsumedNibbles);

        var addr = Data.Addresses[index];

        if (addr.IsNull)
        {
            var newPage = batch.GetNewPage(out addr, true);

            Data.Addresses[index] = addr;

            newPage.Header.PageType = TPageType.Type;
            newPage.Header.Level = (byte)(Header.Level + ConsumedNibbles);

            TPage.Wrap(newPage).Set(sliced, data, batch);
            return page;
        }

        // update after set
        addr = batch.GetAddress(TPage.Wrap(batch.GetAt(addr)).Set(sliced, data, batch));
        Data.Addresses[index] = addr;
        return page;
    }

    public void Report(IReporter reporter, IPageResolver resolver, int pageLevel, int trimmedNibbles)
    {
        var consumedNibbles = trimmedNibbles + ConsumedNibbles;
        foreach (var bucket in Data.Addresses)
        {
            if (!bucket.IsNull)
            {
                TPage.Wrap(resolver.GetAt(bucket)).Report(reporter, resolver, pageLevel + 1, consumedNibbles);
            }
        }
    }

    public void Accept(IPageVisitor visitor, IPageResolver resolver, DbAddress addr)
    {
        using var scope = visitor.On(this, addr);

        foreach (var bucket in Data.Addresses)
        {
            if (!bucket.IsNull)
            {
                TPage.Wrap(resolver.GetAt(bucket)).Accept(visitor, resolver, bucket);
            }
        }
    }
}

public static class FanOutPage
{
    [StructLayout(LayoutKind.Explicit, Size = Size)]
    public struct Payload
    {
        private const int Size = Page.PageSize - PageHeader.Size;

        /// <summary>
        /// The first item of map of frames to allow ref to it.
        /// </summary>
        [FieldOffset(0)] public DbAddressList.Of256 Addresses;
    }
}