using System.Diagnostics.Contracts;
using Paprika.Utils;

namespace Paprika.Chain;

/// <summary>
/// Represents the cache budget per block.
///
/// The cache is transient and is never stored in the database.
/// The cache budget consists of entries per block and the boundary beyond which reads should be cached.
/// Remember that db reads use <see cref="ReadOnlySpanOwnerWithMetadata{T}.DatabaseQueryDepth"/>
/// so that they will always fall into any number.
/// </summary>
public class CacheBudget
{
    public readonly record struct Options(int EntriesPerBlock, ushort CacheFromDepth)
    {
        [Pure]
        public CacheBudget Build() => new(EntriesPerBlock, CacheFromDepth);

        public static Options None => default;
    }

    private readonly ushort _depth;
    private int _entriesPerBlock;

    public int BudgetLeft => _entriesPerBlock;

    private CacheBudget(int entriesPerBlock, ushort depth)
    {
        _entriesPerBlock = entriesPerBlock;
        _depth = depth;
    }

    public bool ShouldCache(in ReadOnlySpanOwnerWithMetadata<byte> owner)
    {
        return owner.QueryDepth >= _depth &&
               // first just read, only then use atomic
               Volatile.Read(ref _entriesPerBlock) > 0 &&
               Interlocked.Decrement(ref _entriesPerBlock) >= 0;
    }
}