namespace Paprika.Store;

public interface IPageVisitor
{
    void On(RootPage page, DbAddress addr);

    void On(AbandonedPage page, DbAddress addr);

    void On(DataPage page, DbAddress addr);

    void On(FanOutPage page, DbAddress addr);
}