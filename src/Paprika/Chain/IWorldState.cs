using Paprika.Crypto;

namespace Paprika.Chain;

/// <summary>
/// Represents the world state of Ethereum at a given block.
/// </summary>
public interface IWorldState : IDisposable
{
    void SetAccount(in Keccak address, in Account account);

    Account GetAccount(in Keccak address);

    /// <summary>
    /// Destroys the given account.
    /// </summary>
    void DestroyAccount(in Keccak address);

    Span<byte> GetStorage(in Keccak address, in Keccak storage, Span<byte> destination);

    void SetStorage(in Keccak address, in Keccak storage, ReadOnlySpan<byte> value);

    /// <summary>
    /// Commits the block to the chain allowing to build upon it.
    /// Also runs the <see cref="IPreCommitBehavior"/> that the blockchain was configured with.
    /// </summary>
    /// <returns>The result of the commit that is actually <see cref="IPreCommitBehavior.BeforeCommit"/> result. </returns>
    Keccak Commit(uint blockNumber);
}