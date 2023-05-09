using Paprika.Db;

namespace Paprika.Data;

public interface IPageVisitor
{
    void On(RootPage page, DbAddress addr);

    void On(AbandonedPage page, DbAddress addr);

    void On(DataPage page, DbAddress addr);

    void On(FanOut256Page page, DbAddress addr);
}