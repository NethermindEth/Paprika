using Paprika.Crypto;
using Paprika.Data;

namespace Paprika;

public interface IBatch : IReadOnlyBatch
{
    /// <summary>
    /// Sets the metadata of the root of the current batch.
    /// </summary>
    void SetMetadata(uint blockNumber, in Keccak blockHash);

    /// <summary>
    /// Sets data raw.
    /// </summary>
    void SetRaw(in Key key, ReadOnlySpan<byte> rawData);

    /// <summary>
    /// Marks the given account as destroyed.
    /// </summary>
    void Destroy(in NibblePath account);

    /// <summary>
    /// Commits the block returning its root hash.
    /// </summary>
    /// <param name="options">How to commit.</param>
    /// <returns>The state root hash.</returns>
    ValueTask Commit(CommitOptions options);
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

    /// <summary>
    /// No write to file. Everything is in-memory only.
    /// </summary>
    DangerNoWrite,
}