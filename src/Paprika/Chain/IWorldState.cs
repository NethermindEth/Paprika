using Nethermind.Int256;
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

    public UInt256 GetStorage(in Keccak key, in Keccak address);

    public void SetAccount(in Keccak key, in Account account);

    public Account GetAccount(in Keccak key);

    public void SetStorage(in Keccak key, in Keccak address, UInt256 value);

    /// <summary>
    /// Commits the block to the block chain.
    /// </summary>
    void Commit();
}