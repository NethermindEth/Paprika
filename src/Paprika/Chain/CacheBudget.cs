using System.Diagnostics.Contracts;

namespace Paprika.Chain;

/// <summary>
/// Represents the cache budget per block.
/// Cache budget consists of transient and db writes.
/// The first are set only in the block, with no impact on the db.
/// The second traverse the whole and are later set in the database.
/// </summary>
public class CacheBudget
{
    public readonly record struct Options(int TransientWrites)
    {
        [Pure]
        public CacheBudget Build() => new(TransientWrites);

        public static Options None => default;
    }

    private int _transientWrites;

    private CacheBudget(int transientWrites)
    {
        _transientWrites = transientWrites;
    }

    public bool ClaimTransient() => Interlocked.Decrement(ref _transientWrites) >= 0;

    public bool IsTransientAvailable => Volatile.Read(ref _transientWrites) > 0;
}