namespace Paprika.Store;

public interface IPageVisitor
{
    IDisposable On(RootPage page, DbAddress addr);

    IDisposable On(AbandonedPage page, DbAddress addr);

    IDisposable On(DataPage page, DbAddress addr);

    IDisposable On(FanOutPage page, DbAddress addr);
    IDisposable On(LeafPage page, DbAddress addr);
    IDisposable On<TNext>(StorageFanOutPage<TNext> page, DbAddress addr)
        where TNext : struct, IPageWithData<TNext>;

    IDisposable On(LeafOverflowPage page, DbAddress addr);

    IDisposable On(Merkle.StateRootPage data, DbAddress addr);
}