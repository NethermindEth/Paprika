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

    public IDisposable On(RootPage page, DbAddress addr) => Build(nameof(RootPage), addr);

    public IDisposable On(AbandonedPage page, DbAddress addr) => Build(nameof(AbandonedPage), addr);


    public IDisposable On(DataPage page, DbAddress addr) => Build(nameof(DataPage), addr, page.CapacityLeft);

    public IDisposable On(FanOutPage page, DbAddress addr) => Build(nameof(FanOutPage), addr);

    public IDisposable On(LeafPage page, DbAddress addr) => Build(nameof(LeafPage), addr, page.CapacityLeft);

    public IDisposable On<TNext>(StorageFanOutPage<TNext> page, DbAddress addr)
        where TNext : struct, IPageWithData<TNext> =>
        Build(nameof(StorageFanOutPage), addr);

    public IDisposable On(LeafOverflowPage page, DbAddress addr) => Build(nameof(LeafOverflowPage), addr, page.CapacityLeft);
    public IDisposable On(Paprika.Store.Merkle.StateRootPage data, DbAddress addr) => Build(nameof(LeafOverflowPage), addr);

    public void Dispose() => _nodes.TryPop(out _);
}