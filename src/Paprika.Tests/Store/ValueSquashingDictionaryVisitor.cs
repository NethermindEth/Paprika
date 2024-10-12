using System.Runtime.InteropServices;
using Paprika.Data;
using Paprika.Store;
using Paprika.Utils;

namespace Paprika.Tests.Store;

/// <summary>
/// A visitor that allows to squash all the values in a given database to a single dictionary so that they can be expected more easly.
/// </summary>
/// <param name="resolver"></param>
public class ValueSquashingDictionaryVisitor(IPageResolver resolver) : IPageVisitor
{
    public readonly Dictionary<byte[], byte[]> Dictionary = new(new BytesEqualityComparer());

    private void ReportMap(ref NibblePath.Builder prefix, in SlottedArray map)
    {
        Span<byte> span = stackalloc byte[64];

        foreach (var item in map.EnumerateAll())
        {
            var key = prefix.Append(item.Key).WriteTo(span).ToArray();

            ref var value = ref CollectionsMarshal.GetValueRefOrAddDefault(Dictionary, key, out var exists);
            if (exists == false)
            {
                // Set only if does not exist
                value = item.RawData.ToArray();
            }
        }
    }

    public IDisposable On<TPage>(scoped ref NibblePath.Builder prefix, TPage page, DbAddress addr)
        where TPage : unmanaged, IPage
    {
        var p = page.AsPage();
        switch (p.Header.PageType)
        {
            case PageType.DataPage:
                ReportMap(ref prefix, new DataPage(p).Map);
                break;
            case PageType.LeafOverflow:
                ReportMap(ref prefix, new LeafOverflowPage(p).Map);
                break;
        }

        return NoopDisposable.Instance;
    }

    public IDisposable On<TPage>(TPage page, DbAddress addr)
        where TPage : unmanaged, IPage =>
        NoopDisposable.Instance;

    public IDisposable Scope(string name) => NoopDisposable.Instance;
}