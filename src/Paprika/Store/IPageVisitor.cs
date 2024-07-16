namespace Paprika.Store;

public interface IPageVisitor
{
    IDisposable On<TPage>(in TPage page, DbAddress addr);
}

public sealed class Disposable : IDisposable
{
    private Disposable()
    {
    }

    public static readonly IDisposable Instance = new Disposable();

    public void Dispose()
    {

    }
}