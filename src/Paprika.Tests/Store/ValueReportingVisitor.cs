using Paprika.Data;
using Paprika.Store;
using Spectre.Console;

namespace Paprika.Tests.Store;

/// <summary>
/// Reports the structure of the tree as a <see cref="Tree"/>.
/// </summary>
/// <param name="resolver"></param>
public class ValueReportingVisitor(IPageResolver resolver) : IPageVisitor, IDisposable
{
    public readonly Tree Tree = new("Db");

    private readonly Stack<TreeNode> _nodes = new();

    private (IDisposable scope, Paragraph text, TreeNode node) Build()
    {
        var text = new Paragraph();
        var node = new TreeNode(text);

        if (_nodes.TryPeek(out var parent))
        {
            parent.AddNode(node);
        }
        else
        {
            Tree.AddNode(node);
        }

        _nodes.Push(node);
        return (this, text, node);
    }

    private static void ReportMap(TreeNode node, in SlottedArray map)
    {
        foreach (var item in map.EnumerateAll())
        {
            var v = item.RawData.Length == 0 ? "delete" : "value";
            node.AddNode(new Text($"[{item.Key.ToString()}]:[{v}]"));
        }
    }

    public IDisposable On<TPage>(scoped ref NibblePath.Builder prefix, TPage page, DbAddress addr)
        where TPage : unmanaged, IPage
    {
        var (scope, text, node) = Build();

        text.Append(FormatNodeNameForPage(page, addr));

        var p = page.AsPage();
        switch (p.Header.PageType)
        {
            case PageType.DataPage:
                ReportMap(node, new DataPage(p).Map);
                break;
            case PageType.Bottom:
                ReportMap(node, new BottomPage(p).Map);
                break;
        }

        return scope;
    }

    public IDisposable On<TPage>(TPage page, DbAddress addr)
        where TPage : unmanaged, IPage
    {
        var (scope, text, _) = Build();
        text.Append(FormatNodeNameForPage(page, addr));
        return scope;
    }

    public IDisposable Scope(string name)
    {
        var (scope, text, _) = Build();
        text.Append(name);
        return scope;
    }

    private static string FormatNodeNameForPage<TPage>(in TPage page, DbAddress addr)
        where TPage : struct, IPage
    {
        return $"{page.GetType().Name.Replace("Page", "")}, @{addr.Raw}";
    }

    public void Dispose() => _nodes.TryPop(out _);
}