using Paprika.Crypto;
using Paprika.Data;
using Paprika.Merkle;

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

    Keccak GetStorageHash(in Keccak account, in NibblePath path, bool ignoreCache);

    Keccak RecalculateRootHash();

    bool IsPersisted(in Keccak account, NibblePath path);

    void CreateMerkleBranch(in Keccak account, in NibblePath storagePath, byte[] childNibbles, Keccak[] childHashes, bool persist = true);
    void CreateMerkleExtension(in Keccak account, in NibblePath storagePath, in NibblePath extPath, bool persist = true);
    void CreateMerkleLeaf(in Keccak account, in NibblePath storagePath, in NibblePath leafPath);

    void ProcessProofNodes(in Keccak account, Span<byte> packedProofPaths, int proofCount);

    /// <summary>
    /// Registers a deletion that will be applied when <see cref="Commit"/> is called.
    /// </summary>
    /// <param name="prefix"></param>
    void RegisterDeleteByPrefix(in Key prefix);

    /// <summary>
    /// Commits the pending changes.
    /// </summary>
    void Commit(bool ensureHash = true, bool keepOpened = false);

    /// <summary>
    /// Open new readonly transaction and create state
    /// </summary>
    public void Open();

    /// <summary>
    /// Finalizes the raw state flushing the metadata.
    /// </summary>
    void Finalize(uint blockNumber);

    /// <summary>
    /// Enforces root hash calculation without actual commit
    /// </summary>
    /// <returns></returns>
    Keccak RefreshRootHash(bool isSyncMode = false);

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

    /// <summary>
    /// Accept a merkle trie visitor and traverse trie from given path
    /// </summary>
    /// <param name="visitor"></param>
    /// <param name="rootPath"></param>
    void Accept(IMerkleTrieVisitor visitor, NibblePath rootPath);
}
