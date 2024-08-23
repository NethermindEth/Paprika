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
    /// Registers a deletion that will be applied when <see cref="Commit"/> is called.
    /// </summary>
    /// <param name="prefix"></param>
    void RegisterDeleteByPrefix(in Key prefix);

    /// <summary>
    /// Commits the pending changes.
    /// </summary>
    void Commit();

    /// <summary>
    /// Finalizes the raw state flushing the metadata.
    /// </summary>
    void Finalize(uint blockNumber);
}
