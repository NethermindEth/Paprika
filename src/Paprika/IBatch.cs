using Paprika.Crypto;

namespace Paprika;

public interface IBatch : IDisposable
{
    /// <summary>
    /// Gets the account information
    /// </summary>
    /// <param name="key"></param>
    /// <returns></returns>
    Account GetAccount(in Keccak key);

    /// <summary>
    /// Sets the given account.
    /// </summary>
    /// <param name="key"></param>
    /// <param name="account"></param>
    void Set(in Keccak key, in Account account);

    /// <summary>
    /// Commits the block returning its root hash.
    /// </summary>
    /// <param name="options">How to commit.</param>
    /// <returns>The root hash.</returns>
    Keccak Commit(CommitOptions options);

    double TotalUsedPages { get; }
}