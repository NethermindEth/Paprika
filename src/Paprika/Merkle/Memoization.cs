namespace Paprika.Merkle;

/// <summary>
/// What should be memoized in Merkle as transient, to speed up the compute.
/// </summary>
public enum Memoization
{
    /// <summary>
    /// Nothing.
    /// </summary>
    None = 0,

    /// <summary>
    /// 512bytes per branch, copied over between commits. Can cost a lot in terms of memory being copied.
    /// </summary>
    Branch = 1,
}