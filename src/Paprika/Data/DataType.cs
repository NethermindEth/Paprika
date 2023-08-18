namespace Paprika.Data;

/// <summary>
/// Represents the type of data stored in the <see cref="NibbleBasedMap"/>.
/// </summary>
public enum DataType : byte
{
    /// <summary>
    /// [key, 0]-> account (balance, nonce, codeHash, storageRootHash)
    /// </summary>
    Account = 0,

    /// <summary>
    /// [key, 1]-> [index][value],
    /// add to the key and use first 32 bytes of data as key
    /// </summary>
    StorageCell = 1,

    /// <summary>
    /// [key, 2]-> the DbAddress of the root page of the storage trie,
    /// </summary>
    StorageTreeRootPageAddress = 2,

    /// <summary>
    /// [storageCellIndex, 3]-> the StorageCell index, without the prefix of the account
    /// </summary>
    StorageTreeStorageCell = 3,

    /// <summary>
    /// As enums cannot be partial, this is for storing the Merkle.
    /// </summary>
    Merkle = 4,

    // 5, 6 - available
    
    /// <summary>
    /// Special type for <see cref="NibbleBasedMap"/>. 
    /// </summary>
    Deleted = 7,
}