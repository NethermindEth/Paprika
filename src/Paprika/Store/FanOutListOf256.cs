using Paprika.Data;

namespace Paprika.Store;

/// <summary>
/// Provides a convenient data structure for <see cref="RootPage"/>,
/// to hold a list of child addresses of <see cref="DbAddressList.IDbAddressList"/> but with addition of
/// handling the updates to addresses.
/// </summary>
public readonly ref struct FanOutListOf256<TPage, TPageType>(ref DbAddressList.Of256 addresses)
    where TPage : struct, IPageWithData<TPage>
    where TPageType : IPageTypeProvider
{
    private readonly ref DbAddressList.Of256 _addresses = ref addresses;
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

        return FanOutPage<TPage, TPageType>.Wrap(batch.GetAt(addr)).TryGet(batch, key.SliceFrom(ConsumedNibbles), out result);
    }

    private static int GetIndex(scoped in NibblePath key) => (key.GetAt(0) << NibblePath.NibbleShift) + key.GetAt(1);

    public void Set(in NibblePath key, in ReadOnlySpan<byte> data, IBatchContext batch)
    {
        var index = GetIndex(key);
        var sliced = key.SliceFrom(ConsumedNibbles);

        var addr = _addresses[index];

        if (addr.IsNull)
        {
            var newPage = batch.GetNewPage(out addr, true);
            _addresses[index] = addr;

            newPage.Header.PageType = PageType.FanOutPage;
            newPage.Header.Level = 2;

            FanOutPage<TPage, TPageType>.Wrap(newPage).Set(sliced, data, batch);
            return;
        }

        // The page exists, update
        var updated = FanOutPage<TPage, TPageType>.Wrap(batch.GetAt(addr)).Set(sliced, data, batch);
        _addresses[index] = batch.GetAddress(updated);
    }

    public void Report(IReporter reporter, IPageResolver resolver, int level, int trimmedNibbles)
    {
        var consumedNibbles = trimmedNibbles + ConsumedNibbles;

        foreach (var bucket in _addresses)
        {
            if (!bucket.IsNull)
            {
                FanOutPage<TPage, TPageType>.Wrap(resolver.GetAt(bucket)).Report(reporter, resolver, level + 1, consumedNibbles);
            }
        }
    }

    public void Accept(IPageVisitor visitor, IPageResolver resolver)
    {
        foreach (var bucket in _addresses)
        {
            if (!bucket.IsNull)
            {
                FanOutPage<TPage, TPageType>.Wrap(resolver.GetAt(bucket)).Accept(visitor, resolver, bucket);
            }
        }
    }
}

