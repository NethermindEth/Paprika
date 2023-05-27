using Nethermind.Int256;
using Paprika.Crypto;
using Paprika.Data;

namespace Paprika;

public interface IReadOnlyBatch : IDisposable
{
    /// <summary>
    /// Low level retrieval of data.
    /// </summary>
    /// <param name="key"></param>
    /// <param name="result"></param>
    /// <returns></returns>
    bool TryGet(in Key key, out ReadOnlySpan<byte> result);
}

public static class ReadExtensions
{
    /// <summary>
    /// Gets the account information
    /// </summary>
    /// <param name="batch">The batch to read from.</param>
    /// <param name="key">The key to looked up.</param>
    /// <returns>The account or default on non-existence.</returns>
    public static Account GetAccount(this IReadOnlyBatch batch, in Keccak key)
    {
        if (batch.TryGet(Key.Account(NibblePath.FromKey(key)), out var result))
        {
            Serializer.ReadAccount(result, out var account);
            return account;
        }

        return default;
    }

    /// <summary>
    /// Gets the storage value.
    /// </summary>
    public static UInt256 GetStorage(this IReadOnlyBatch batch, in Keccak account, in Keccak address)
    {
        if (batch.TryGet(Key.StorageCell(NibblePath.FromKey(account), address), out var result))
        {
            Serializer.ReadStorageValue(result, out var value);
            return value;
        }

        return default;
    }

}
