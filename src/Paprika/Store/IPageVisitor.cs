namespace Paprika.Store;

public interface IPageVisitor
{
    IDisposable On(RootPage page, DbAddress addr);

    IDisposable On(AbandonedPage page, DbAddress addr);

    IDisposable On(DataPage page, DbAddress addr);

    IDisposable On(FanOutPage page, DbAddress addr);
    IDisposable On(LeafPage page, DbAddress addr);

    IDisposable On(LeafOverflowPage page, DbAddress addr);
}