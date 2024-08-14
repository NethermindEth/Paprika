using Paprika.Store;
using Spectre.Console;

namespace Paprika.Tests.Store;

public class TreeView : IPageVisitor, IDisposable
{
    public readonly Tree Tree = new("Db");

    private readonly Stack<TreeNode> _nodes = new();

    private IDisposable Build(string name, DbAddress? addr, int? capacityLeft = null)
    {
        string text;
        if (addr.HasValue)
        {
            var capacity = capacityLeft.HasValue ? $", space_left: {capacityLeft.Value}" : "";
            text = $"{name.Replace("Page", "")}, @{addr.Value.Raw}{capacity}";
        }
        else
        {
            text = name;
        }

        var node = new TreeNode(new Text(text));

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

    public IDisposable On<TPage>(in TPage page, DbAddress addr) => Build(page.GetType().Name, addr);
    public IDisposable Scope(string name) => Build(name, null);

    public void Dispose() => _nodes.TryPop(out _);
}