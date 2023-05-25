namespace Paprika.Utils;

/// <summary>
/// Provides a component that can be disposed multiple times and runs <see cref="CleanUp"/> only on the last dispose. 
/// </summary>
public abstract class RefCountingDisposable : IDisposable
{
    private const int Initial = 1;
    private const int NoAccessors = 0;
    private const int Disposing = 1 << 31;

    private int _counter;

    protected RefCountingDisposable(int initialCount = Initial)
    {
        _counter = initialCount;
    }

    protected bool TryAcquireLease()
    {
        var value = Interlocked.Increment(ref _counter);
        if ((value & Disposing) == Disposing)
        {
            // move back as the component is being disposed
            Interlocked.Decrement(ref _counter);

            return false;
        }

        return true;
    }

    /// <summary>
    /// Disposes it once, decreasing the lease count by 1.
    /// </summary>
    public void Dispose() => ReleaseLeaseOnce();

    protected void ReleaseLeaseOnce()
    {
        var value = Interlocked.Decrement(ref _counter);

        if (value == NoAccessors)
        {
            if (Interlocked.CompareExchange(ref _counter, Disposing, NoAccessors) == NoAccessors)
            {
                // set to disposed by this Release
                CleanUp();
            }
        }
    }

    protected abstract void CleanUp();
}