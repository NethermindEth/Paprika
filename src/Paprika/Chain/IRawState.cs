using Paprika.Crypto;

namespace Paprika.Chain;

/// <summary>
/// Allows raw data state for syncing purposes.
/// </summary>
public interface IRawState : IDisposable
{
    Account GetAccount(in Keccak address);

    Span<byte> GetStorage(in Keccak address, in Keccak storage, Span<byte> destination);

    void SetAccount(in Keccak address, in Account account);

    void SetStorage(in Keccak address, in Keccak storage, ReadOnlySpan<byte> value);

    /// <summary>
    /// Commits the pending changes.
    /// </summary>
    void Commit();
}