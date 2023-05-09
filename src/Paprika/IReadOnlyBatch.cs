using Nethermind.Int256;
using Paprika.Crypto;

namespace Paprika;

public interface IReadOnlyBatch : IDisposable
{
    /// <summary>
    /// Gets the account information
    /// </summary>
    /// <param name="key">The key to looked up.</param>
    /// <returns>The account or default on non-existence.</returns>
    Account GetAccount(in Keccak key);

    /// <summary>
    /// Gets the storage value.
    /// </summary>
    UInt256 GetStorage(in Keccak key, in Keccak address);
}