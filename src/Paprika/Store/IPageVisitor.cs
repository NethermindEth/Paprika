namespace Paprika.Store;

public interface IPageVisitor
{
    IDisposable On<TPage>(in TPage page, DbAddress addr);

    /// <summary>
    /// Just a named scope, to group pages.
    /// </summary>
    IDisposable Scope(string name);
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