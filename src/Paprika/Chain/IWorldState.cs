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

    /// <summary>
    /// Sets the account. If the caller is sure that this is a new account,
    /// pass the <paramref name="newAccountHint"/> to reduce the overhead of the creation.
    /// </summary>
    void SetAccount(in Keccak address, in Account account, bool newAccountHint = false);

    /// <summary>
    /// Destroys the given account.
    /// </summary>
    void DestroyAccount(in Keccak address);

    void SetStorage(in Keccak address, in Keccak storage, ReadOnlySpan<byte> value);

    /// <summary>
    /// Tries to prefetch some data for the <see cref="IPreCommitBehavior"/>. 
    /// </summary>
    /// <param name="address">The hash of the address that the storage is written to</param>
    /// <param name="storage">The storage slot that might be written to.</param>
    /// <returns>Whether follow-up prefetches should be issued by the caller.</returns>
    /// <remarks>
    /// This operation can and should be called from another thread in parallel to other operations in a preparation of calling <see cref="Commit"/>.
    /// Once <see cref="Commit"/> is called, no more prefetching is possible.
    /// </remarks>
    bool TryPrefetchForPreCommit(in Keccak address, in Keccak storage);

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
