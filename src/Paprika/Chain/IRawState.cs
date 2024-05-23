using Paprika.Crypto;
using Paprika.Data;

namespace Paprika.Chain;

/// <summary>
/// Allows raw data state for syncing purposes.
/// </summary>
public interface IRawState : IReadOnlyWorldState
{
    void SetBoundary(in NibblePath account, in Keccak boundaryNodeKeccak);
    void SetBoundary(in Keccak account, in NibblePath storage, in Keccak boundaryNodeKeccak);

    void SetAccount(in Keccak address, in Account account);

    void SetStorage(in Keccak address, in Keccak storage, ReadOnlySpan<byte> value);

    void DestroyAccount(in Keccak address);

    /// <summary>
    /// Commits the pending changes.
    /// </summary>
    void Commit(bool ensureHash = true);

    /// <summary>
    /// Finalizes the raw state flushing the metadata.
    /// </summary>
    void Finalize(uint blockNumber);

    /// <summary>
    /// Enforces root hash calculation without actual commit
    /// </summary>
    /// <returns></returns>
    Keccak RefreshRootHash();

    /// <summary>
    /// Recalculates storage roots and returns new storage root hash for a given account 
    /// </summary>
    /// <param name="accountAddress"></param>
    /// <returns></returns>
    Keccak RecalculateStorageRoot(in Keccak accountAddress);

    /// <summary>
    /// Cleans current data
    /// </summary>
    void Discard();
}
