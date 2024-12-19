using Paprika.Crypto;

namespace Paprika.Chain;

/// <summary>
/// Represents the world state of Ethereum at a given block.
/// </summary>
public interface IWorldState : IStateStorageAccessor, IDisposable
{
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
    /// Gets the storage setter, so that some of the heavy calls for the storage can be amortized. 
    /// </summary>
    /// <param name="address"></param>
    /// <returns></returns>
    IStorageSetter GetStorageSetter(in Keccak address);

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

    public IPreCommitPrefetcher? OpenPrefetcher();
}

public interface IStorageSetter
{
    /// <summary>
    /// Sets the storage in the same way <see cref="IWorldState.SetStorage"/> does.
    /// </summary>
    void SetStorage(in Keccak storage, ReadOnlySpan<byte> value);
}

public interface IReadOnlyWorldState : IStateStorageAccessor, IReadOnlyCommit, IDisposable
{
    /// <summary>
    /// Gets the current hash of the world state.
    /// </summary>
    Keccak Hash { get; }
}

/// <summary>
/// A shared interface between <see cref="IReadOnlyWorldState"/>
/// and <see cref="IWorldState"/>.
/// </summary>
public interface IStateStorageAccessor
{
    Account GetAccount(in Keccak address);

    Span<byte> GetStorage(in Keccak address, in Keccak storage, Span<byte> destination);
}

/// <summary>
/// Provides the access to the state similar to <see cref="IReadOnlyWorldState"/> but accepting the root hash that it should be searched for.
/// </summary>
public interface IReadOnlyWorldStateAccessor
{
    bool HasState(in Keccak rootHash);

    Account GetAccount(in Keccak rootHash, in Keccak address);

    Span<byte> GetStorage(in Keccak rootHash, in Keccak address, in Keccak storage, Span<byte> destination);
}