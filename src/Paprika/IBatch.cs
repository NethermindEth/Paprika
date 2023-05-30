﻿using Nethermind.Int256;
using Paprika.Crypto;
using Paprika.Store;

namespace Paprika;

public interface IBatch : IReadOnlyBatch
{
    /// <summary>
    /// Sets the metadata of the root of the current batch.
    /// </summary>
    void SetMetadata(uint blockNumber, in Keccak blockHash);

    /// <summary>
    /// Sets the given account.
    /// </summary>
    /// <param name="key"></param>
    /// <param name="account"></param>
    void Set(in Keccak key, in Account account);

    /// <summary>
    /// Sets storage.
    /// </summary>
    void SetStorage(in Keccak key, in Keccak address, UInt256 value);

    /// <summary>
    /// Commits the block returning its root hash.
    /// </summary>
    /// <param name="options">How to commit.</param>
    /// <returns>The state root hash.</returns>
    ValueTask Commit(CommitOptions options);

    /// <summary>
    /// Applies roots in raw form on the root of the batch.
    /// </summary>
    void Apply(DbAddress[] externalRoots, IPageResolver externalPageResolver);
}

public enum CommitOptions
{
    /// <summary>
    /// Flushes db only once, ensuring that the data are stored properly.
    /// The root is stored ephemerally, waiting for the next commit to be truly stored.
    ///
    /// This guarantees ATOMIC (from ACID) but not DURABLE.  
    /// </summary>
    FlushDataOnly,

    /// <summary>
    /// Flush data, then the root.
    ///
    /// This guarantees ATOMIC and DURABLE (from ACID).
    /// </summary>
    FlushDataAndRoot,

    /// <summary>
    /// No actual flush happens and the database may become corrupted when the program is interrupted.
    /// </summary>
    DangerNoFlush,
}