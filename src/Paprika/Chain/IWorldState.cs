using Paprika.Crypto;

namespace Paprika.Chain;

/// <summary>
/// Represents the world state of Ethereum at a given block.
/// </summary>
public interface IWorldState : IDisposable
{
    Account GetAccount(in Keccak address);

    Span<byte> GetStorage(in Keccak address, in Keccak storage, Span<byte> destination);

    /// <summary>
    /// Gets the current hash of the world state.
    /// </summary>
    Keccak Hash { get; }

    void SetAccount(in Keccak address, in Account account);

    /// <summary>
    /// Destroys the given account.
    /// </summary>
    void DestroyAccount(in Keccak address);

    void SetStorage(in Keccak address, in Keccak storage, ReadOnlySpan<byte> value);

    /// <summary>
    /// Commits the block to the chain allowing to build upon it.
    /// Also runs the <see cref="IPreCommitBehavior"/> that the blockchain was configured with.
    /// </summary>
    /// <returns>The result of the commit that is actually <see cref="IPreCommitBehavior.BeforeCommit"/> result. </returns>
    Keccak Commit(uint blockNumber);

    /// <summary>
    /// Cleans up all the changes in the world state.
    /// </summary>
    void Reset();

    public IStateStats Stats { get; }
}

public interface IReadOnlyWorldState : IReadOnlyCommit, IDisposable
{
    Account GetAccount(in Keccak address);

    Span<byte> GetStorage(in Keccak address, in Keccak storage, Span<byte> destination);

    /// <summary>
    /// Gets the current hash of the world state.
    /// </summary>
    Keccak Hash { get; }
}