using Paprika.Crypto;

namespace Paprika;

public interface IBatch : IReadOnlyBatch
{
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
    /// <returns>The state root hash.</returns>
    Keccak Commit(CommitOptions options);
}

public interface IReadOnlyBatch : IDisposable
{
    /// <summary>
    /// Gets the account information
    /// </summary>
    /// <param name="key">The key to looked up.</param>
    /// <returns>The account or default on non-existence.</returns>
    Account GetAccount(in Keccak key);
}