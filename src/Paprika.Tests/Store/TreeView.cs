using Paprika.Store;
using Spectre.Console;

namespace Paprika.Tests.Store;

public class TreeView : IPageVisitor, IDisposable
{
    public readonly Tree Tree = new("Db");

    private readonly Stack<TreeNode> _nodes = new();

    private IDisposable Build(string name, DbAddress addr, int? capacityLeft = null)
    {
        var count = _nodes.Count;
        var capacity = capacityLeft.HasValue ? $", space_left: {capacityLeft.Value}" : "";
        var text = $"{count}: {name.Replace("Page", "")}, @{addr.Raw}{capacity}";

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

    public IDisposable On<TPage>(in TPage page, DbAddress addr) => Build(nameof(TPage), addr);

    public void Dispose() => _nodes.TryPop(out _);
}