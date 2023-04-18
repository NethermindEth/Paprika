namespace Paprika.Pages;

public interface IPageVisitor
{
    void On(RootPage page, DbAddress addr);

    void On(AbandonedPage page, DbAddress addr);

    void On(DataPage page, DbAddress addr);
}