using Nethermind.Int256;
using Paprika.Crypto;

namespace Paprika;

public interface IBatch : IDisposable
{
    bool TryGetNonce(in Keccak account, out UInt256 nonce);

    void Set(in Keccak account, in UInt256 balance, in UInt256 nonce);

    /// <summary>
    /// Commits the block returning its root hash.
    /// </summary>
    /// <param name="options">How to commit.</param>
    /// <returns>The root hash.</returns>
    Keccak Commit(CommitOptions options);

    double TotalUsedPages { get; }
}