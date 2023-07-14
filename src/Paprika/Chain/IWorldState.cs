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

    public UInt256 GetStorage(in Keccak account, in Keccak address);

    public void SetAccount(in Keccak key, in Account account);

    public Account GetAccount(in Keccak account);

    public void SetStorage(in Keccak key, in Keccak address, UInt256 value);

    /// <summary>
    /// Commits the block to the chain allowing to build upon it.
    /// Also runs the <see cref="IPreCommitBehavior"/> that the blockchain was configured with.
    /// </summary>
    void Commit();
}