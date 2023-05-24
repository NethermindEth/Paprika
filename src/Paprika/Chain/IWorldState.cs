using Paprika.Crypto;

namespace Paprika.Chain;

/// <summary>
/// Represents the world state of Ethereum at a given block.
/// </summary>
public interface IWorldState : IDisposable
{
    Keccak Hash { get; }
    Keccak ParentHash { get; }
    uint BlockNumber { get; }

    /// <summary>
    /// Commits the block to the block chain.
    /// </summary>
    void Commit();
}