namespace Paprika.Utils;

/// <summary>
/// Provides a component that can be disposed multiple times and runs <see cref="CleanUp"/> only on the last dispose. 
/// </summary>
public abstract class RefCountingDisposable : IDisposable
{
    private const int Initial = 1;
    private const int NoAccessors = 0;
    private const int Disposing = -1;
    private const int DisposingBarrier = 0;

    private int _leases;

    protected RefCountingDisposable(int initialCount = Initial)
    {
        _leases = initialCount;
    }

    public void AcquireLease()
    {
        if (TryAcquireLease() == false)
        {
            throw new Exception("The lease cannot be acquired");
        }
    }

    protected bool TryAcquireLease()
    {
        var value = Interlocked.Increment(ref _leases);
        var previous = value - 1;
        if (previous < DisposingBarrier)
        {
            // move back as the component is being disposed
            Interlocked.Decrement(ref _leases);

            return false;
        }

        return true;
    }

    /// <summary>
    /// Disposes it once, decreasing the lease count by 1.
    /// </summary>
    public void Dispose() => ReleaseLeaseOnce();

    private void ReleaseLeaseOnce()
    {
        var value = Interlocked.Decrement(ref _leases);

        if (value == NoAccessors)
        {
            if (Interlocked.CompareExchange(ref _leases, Disposing, NoAccessors) == NoAccessors)
            {
                // set to disposed by this Release
                CleanUp();
            }
        }
    }

    protected abstract void CleanUp();

    public override string ToString()
    {
        var leases = Volatile.Read(ref _leases);
        return leases == Disposing ? "Disposed" : $"Leases: {Volatile.Read(ref leases)}";
    }
}
