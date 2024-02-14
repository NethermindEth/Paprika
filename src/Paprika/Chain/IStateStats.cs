using Paprika.Crypto;

namespace Paprika.Chain;

/// <summary>
/// Provides statistics about persistence aspects.
/// </summary>
public interface IStateStats
{
    /// <summary>
    /// Db reads performed by this component since it's start of the lifetime.
    /// </summary>
    public int DbReads { get; }

    /// <summary>
    /// Gets an enumeration of all the ancestor's states that this component depends on.
    /// This does not include the underling database <see cref="IReadOnlyBatch"/>. Only in memory blocks.
    /// </summary>
    public IEnumerable<(uint blockNumber, Keccak hash)> Ancestors { get; }
}