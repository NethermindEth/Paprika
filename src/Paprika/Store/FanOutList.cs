using Paprika.Data;

namespace Paprika.Store;

public interface ISize
{
    static abstract int GetIndex(scoped in NibblePath key);

    static abstract int ConsumedNibbles { get; }
}

public static class FanOut
{
    public struct Of2Nibbles : ISize
    {
        public static int GetIndex(scoped in NibblePath key) => (key.GetAt(0) << NibblePath.NibbleShift) + key.GetAt(1);

        public static int ConsumedNibbles => 2;

        public const int FanOut = 256;

        public const int Size = FanOut * DbAddress.Size;
    }

    public struct Of3Nibbles : ISize
    {
        public static int GetIndex(scoped in NibblePath key) =>
            (key.GetAt(0) << NibblePath.NibbleShift * 2) +
            (key.GetAt(1) << NibblePath.NibbleShift) +
            key.GetAt(2);

        public static int ConsumedNibbles => 3;

        public const int FanOut = 256 * 16;

        public const int Size = FanOut * DbAddress.Size;
    }
}

/// <summary>
/// Provides a convenient data structure for <see cref="RootPage"/>, to preserve a fan out of
/// <see cref="FanOut"/> pages underneath. 
/// </summary>
/// <remarks>
/// The main idea is to limit the depth of the tree by 1 or two and use the space in <see cref="RootPage"/> more.
/// </remarks>
public readonly ref struct FanOutList<TPage, TPageType, TSize>(Span<DbAddress> addresses)
    where TPage : struct, IPageWithData<TPage>
    where TPageType : IPageTypeProvider
    where TSize : struct, ISize
{
    private readonly Span<DbAddress> _addresses = addresses;

    public bool TryGet(IReadOnlyBatchContext batch, scoped in NibblePath key, out ReadOnlySpan<byte> result)
    {
        var index = TSize.GetIndex(key);

        var addr = _addresses[index];
        if (addr.IsNull)
        {
            result = default;
            return false;
        }

        return TPage.Wrap(batch.GetAt(addr)).TryGet(batch, key.SliceFrom(TSize.ConsumedNibbles), out result);
    }

    public void Set(in NibblePath key, in ReadOnlySpan<byte> data, IBatchContext batch)
    {
        var index = TSize.GetIndex(key);
        var sliced = key.SliceFrom(TSize.ConsumedNibbles);

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
        var consumedNibbles = trimmedNibbles + TSize.ConsumedNibbles;

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