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
    void Commit();
}