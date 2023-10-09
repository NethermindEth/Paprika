using Paprika.Crypto;

namespace Paprika.Chain;

/// <summary>
/// Represents the world state of Ethereum at a given block.
/// </summary>
public interface IWorldState : IDisposable
{
    Keccak Hash { get; }

    uint BlockNumber { get; }

    public Span<byte> GetStorage(in Keccak address, in Keccak storage, Span<byte> destination);

    public void SetAccount(in Keccak address, in Account account);

    public Account GetAccount(in Keccak address);

    public void SetStorage(in Keccak address, in Keccak storage, ReadOnlySpan<byte> value);

    /// <summary>
    /// Commits the block to the chain allowing to build upon it.
    /// Also runs the <see cref="IPreCommitBehavior"/> that the blockchain was configured with.
    /// </summary>
    /// <returns>The result of the commit that is actually <see cref="IPreCommitBehavior.BeforeCommit"/> result. </returns>
    object Commit();
}