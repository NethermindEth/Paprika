using Paprika.Crypto;
using Paprika.Data;

namespace Paprika.Chain;

/// <summary>
/// Allows raw data state for syncing purposes.
/// </summary>
public interface IRawState : IReadOnlyWorldState
{
    void SetAccount(in Keccak address, in Account account);

    void SetStorage(in Keccak address, in Keccak storage, ReadOnlySpan<byte> value);

    void DestroyAccount(in Keccak address);

    Keccak GetHash(in NibblePath path, bool ignoreCache);

    Keccak GetStorageHash(in Keccak account, in NibblePath path);

    void CreateMerkleBranch(in Keccak account, in NibblePath storagePath, byte[] childNibbles, Keccak[] childHashes, bool persist = true);
    void CreateMerkleExtension(in Keccak account, in NibblePath storagePath, in NibblePath extPath, bool persist = true);
    void CreateMerkleLeaf(in Keccak account, in NibblePath storagePath, in NibblePath leafPath);

    /// <summary>
    /// Registers a deletion that will be applied when <see cref="Commit"/> is called.
    /// </summary>
    /// <param name="prefix"></param>
    void RegisterDeleteByPrefix(in Key prefix);

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

    string DumpTrie();
}
