using Paprika.Data;
using Paprika.Store;
using Spectre.Console;

namespace Paprika.Tests.Store;

public class TreeView(IPageResolver resolver) : IPageVisitor, IDisposable
{
    public readonly Tree Tree = new("Db");

    private readonly Stack<TreeNode> _nodes = new();

    private IDisposable BuildNode(string name)
    {
        var node = new TreeNode(new Text(name));

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

    public IDisposable On<TPage>(scoped ref NibblePath.Builder prefix, TPage page, DbAddress addr)
        where TPage : unmanaged, IPage =>
        On(page, addr);

    public IDisposable On<TPage>(TPage page, DbAddress addr) where TPage : unmanaged, IPage
    {
        var name = page.GetType().Name;
        var text = $"{name.Replace("Page", "")}, @{addr.Raw}, lvl: {page.AsPage().Header.Level}";

        var p = page.AsPage();

        if (typeof(TPage) == typeof(DataPage))
        {
            text += ReportUsage(p.Cast<DataPage>().Map);
        }
        else if (typeof(TPage) == typeof(LeafOverflowPage))
        {
            text += ReportUsage(p.Cast<LeafOverflowPage>().Map);
        }

        return BuildNode(text);

        string ReportUsage(SlottedArray map)
        {
            var usage = map.CalculateActualSpaceUsed();
            var report = $", Usage: {usage:P}";
            return report;
        }
    }

    public IDisposable Scope(string name) => BuildNode(name);

    public void Dispose() => _nodes.TryPop(out _);
}