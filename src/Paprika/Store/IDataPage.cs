using Nethermind.Int256;
using Paprika.Crypto;
using Paprika.Data;

namespace Paprika.Store;

/// <summary>
/// An interface for a page holding data, capable to <see cref="TryGet"/> and <see cref="Set"/> values.
/// </summary>
public interface IDataPage : IPage
{
    bool TryGet(Key key, IReadOnlyBatchContext batch, out ReadOnlySpan<byte> result);

    Page Set(in SetContext ctx);
}

/// <summary>
/// Extension methods used as a poor man derivation across structs implementing <see cref="IDataPage"/>.
/// </summary>
public static class DataPageExtensions
{
    public static Account GetAccount<TPage>(this TPage page, NibblePath path, IReadOnlyBatchContext ctx)
        where TPage : struct, IDataPage
    {
        var key = Key.Account(path);

        if (page.TryGet(key, ctx, out var result))
        {
            Serializer.ReadAccount(result, out var balance, out var nonce);
            return new Account(balance, nonce);
        }

        return default;
    }

    public static Page SetAccount<TPage>(this TPage page, NibblePath path, in Account account, IBatchContext batch)
        where TPage : struct, IDataPage
    {
        var key = Key.Account(path);

        Span<byte> payload = stackalloc byte[Serializer.BalanceNonceMaxByteCount];
        payload = Serializer.WriteAccount(payload, account.Balance, account.Nonce);
        var ctx = new SetContext(key, payload, batch);
        return page.Set(ctx);
    }

    public static UInt256 GetStorage<TPage>(this TPage page, NibblePath path, in Keccak address,
        IReadOnlyBatchContext ctx)
        where TPage : struct, IDataPage
    {
        var key = Key.StorageCell(path, address);

        if (page.TryGet(key, ctx, out var result))
        {
            Serializer.ReadStorageValue(result, out var value);
            return value;
        }

        return default;
    }

    public static Page SetStorage<TPage>(this TPage page, NibblePath path, in Keccak address, in UInt256 value,
        IBatchContext batch)
        where TPage : struct, IDataPage
    {
        var key = Key.StorageCell(path, address);

        Span<byte> payload = stackalloc byte[Serializer.StorageValueMaxByteCount];
        payload = Serializer.WriteStorageValue(payload, value);

        var ctx = new SetContext(key, payload, batch);
        return page.Set(ctx);
    }
}
