using Nethermind.Int256;
using Paprika.Crypto;
using Paprika.Data;

namespace Paprika;

public interface IReadOnlyBatch : IDisposable
{
    /// <summary>
    /// Gets the account information
    /// </summary>
    /// <param name="key">The key to looked up.</param>
    /// <returns>The account or default on non-existence.</returns>
    Account GetAccount(in Keccak key)
    {
        if (TryGet(Key.Account(NibblePath.FromKey(key)), out var result))
        {
            Serializer.ReadAccount(result, out var balance, out var nonce);
            return new Account(balance, nonce);
        }

        return default;
    }

    /// <summary>
    /// Gets the storage value.
    /// </summary>
    UInt256 GetStorage(in Keccak account, in Keccak address)
    {
        if (TryGet(Key.StorageCell(NibblePath.FromKey(account), address), out var result))
        {
            Serializer.ReadStorageValue(result, out var value);
            return value;
        }

        return default;
    }

    /// <summary>
    /// Low level retrieval of data.
    /// </summary>
    /// <param name="key"></param>
    /// <param name="result"></param>
    /// <returns></returns>
    bool TryGet(in Key key, out ReadOnlySpan<byte> result);
}
