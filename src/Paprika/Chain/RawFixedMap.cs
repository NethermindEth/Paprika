using Paprika.Data;
using Paprika.Store;

namespace Paprika.Chain;

public readonly struct RawFixedMap
{
    private readonly Page _page;

    public RawFixedMap(Page page) => _page = page;

    public bool TrySet(in Key key, ReadOnlySpan<byte> data)
    {
        var map = new FixedMap(_page.Span);
        return map.TrySet(key, data);
    }

    public bool TryGet(in Key key, out ReadOnlySpan<byte> result)
    {
        var map = new FixedMap(_page.Span);
        return map.TryGet(key, out result);
    }

    public void Apply(IBatch batch)
    {
        var map = new FixedMap(_page.Span);
        foreach (var item in map.EnumerateAll())
        {
            batch.SetRaw(item.Key, item.RawData);
        }
    }
}