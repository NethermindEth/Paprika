using Paprika.Chain;

namespace Paprika.Merkle;

/// <summary>
/// What should be memoized in Merkle as transient, to speed up the compute.
/// </summary>
public enum Memoization
{
    /// <summary>
    /// Nothing. The memoization will default to the <see cref="CacheBudget"/> available for Merkle and
    /// will not create additional structures.
    /// </summary>
    None = 0,

    /// <summary>
    /// Merkle component will additionally create a 512bytes per branch, copied over between commits.
    /// Can cost a lot in terms of memory being copied. Tests showed reduced usefulness after introduction of
    /// large enough <see cref="CacheBudget"/>. Much less copying, less memory occupied and less memory fragmentation.
    /// </summary>
    Branch = 1,
}