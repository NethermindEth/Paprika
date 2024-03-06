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

    public bool TryGet(scoped NibblePath key, IReadOnlyBatchContext batch, out ReadOnlySpan<byte> result)
    {
        var index = GetIndex(key);

        var addr = _addresses[index];
        if (addr.IsNull)
        {
            result = default;
            return false;
        }

        return TPage.Wrap(batch.GetAt(addr)).TryGet(key.SliceFrom(ConsumedNibbles), batch, out result);
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

    public void Report(IReporter reporter, IPageResolver resolver, int level)
    {
        foreach (var bucket in _addresses)
        {
            if (!bucket.IsNull)
            {
                TPage.Wrap(resolver.GetAt(bucket)).Report(reporter, resolver, level + 1);
            }
        }
    }
}
