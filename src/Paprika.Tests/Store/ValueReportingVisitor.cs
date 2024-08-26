using Paprika.Data;
using Paprika.Store;
using Spectre.Console;

namespace Paprika.Tests.Store;

public class ValueReportingVisitor(IPageResolver resolver) : IPageVisitor, IDisposable
{
    private readonly IPageResolver _resolver = resolver;
    public readonly Tree Tree = new("Db");

    private readonly Stack<TreeNode> _nodes = new();

    private IDisposable Build(string name, DbAddress? addr, int? capacityLeft = null)
    {
        TreeNode node;

        if (addr.HasValue)
        {
            var capacity = capacityLeft.HasValue ? $", space_left: {capacityLeft.Value}" : "";
            var dbAddr = addr.Value;

            var text = $"{name.Replace("Page", "")}, @{dbAddr.Raw}{capacity}";
            node = new TreeNode(new Text(text));

            var page = _resolver.GetAt(dbAddr);
            switch (page.Header.PageType)
            {
                case PageType.Standard:
                    ReportMap(node, new DataPage(page).Map);
                    break;
                case PageType.LeafOverflow:
                    ReportMap(node, new LeafOverflowPage(page).Map);
                    break;
            }
        }
        else
        {
            node = new TreeNode(new Text(name));
        }

        if (_nodes.TryPeek(out var parent))
        {
            parent.AddNode(node);
        }
        else
        {
            Tree.AddNode(node);
        }

        _nodes.Push(node);
        return this;
    }

    private static void ReportMap(TreeNode node, in SlottedArray map)
    {
        foreach (var item in map.EnumerateAll())
        {
            var v = item.RawData.Length == 0 ? "delete" : "value";
            node.AddNode(new Text($"[{item.Key.ToString()}]:[{v}]"));
        }
    }

    public IDisposable On<TPage>(in TPage page, DbAddress addr) => Build(page.GetType().Name, addr);
    public IDisposable Scope(string name) => Build(name, null);

    public void Dispose() => _nodes.TryPop(out _);
}