using FluentAssertions;
using Nethermind.Int256;
using Paprika.Crypto;
using Paprika.Data;
using Paprika.Store;

namespace Paprika.Tests;

/// <summary>
/// All methods in here are helpful in testing but are not needed in the core of Paprika as they breach interfaces,
/// assumptions and boundaries. The level of breach is different for different methods, but still, keeping them in one
/// shameful place is easier for both testing and keeping Paprika clean.
/// </summary>
public static class TestExtensions
{
    public static void ShouldHaveAccount(this IReadOnlyBatch read, in Keccak key, ReadOnlySpan<byte> expected)
    {
        read.TryGet(Key.Account(key), out var value).Should().BeTrue();
        value.SequenceEqual(expected);
    }

    public static void ShouldHaveAccount(this IReadOnlyBatch read, in Keccak key, in Account expected)
    {
        Span<byte> payload = stackalloc byte[Serializer.BalanceNonceMaxByteCount];
        var raw = Serializer.WriteAccount(payload, expected);

        read.TryGet(Key.Account(key), out var value).Should().BeTrue();
        value.SequenceEqual(raw);
    }

    public static Account GetAccount(this IReadOnlyBatch read, in Keccak key)
    {
        read.TryGet(Key.Account(key), out var value).Should().BeTrue();
        Serializer.ReadAccount(value, out var account);
        return account;
    }

    public static UInt256 GetStorage(this IReadOnlyBatch read, in Keccak key, in Keccak storage)
    {
        read.TryGet(Key.StorageCell(NibblePath.FromKey(key), storage), out var value).Should().BeTrue();
        Serializer.ReadStorageValue(value, out var v);
        return v;
    }

    public static void ShouldHaveStorage(this IReadOnlyBatch read, in Keccak key, in Keccak storage,
        ReadOnlySpan<byte> expected)
    {
        read.TryGet(Key.StorageCell(NibblePath.FromKey(key), storage), out var value).Should().BeTrue();
        value.SequenceEqual(expected);
    }

    public static void ShouldHaveStorage(this IReadOnlyBatch read, in Keccak key, in Keccak storage,
        UInt256 expected)
    {
        Span<byte> payload = stackalloc byte[Serializer.StorageValueMaxByteCount];
        var raw = Serializer.WriteStorageValue(payload, expected);

        read.TryGet(Key.StorageCell(NibblePath.FromKey(key), storage), out var value).Should().BeTrue();
        value.SequenceEqual(raw);
    }

    public static void SetAccount(this IBatch batch, in Keccak key, ReadOnlySpan<byte> value) =>
        batch.SetRaw(Key.Account(key), value);

    public static void SetStorage(this IBatch batch, in Keccak key, in Keccak storage, ReadOnlySpan<byte> value) =>
        batch.SetRaw(Key.StorageCell(NibblePath.FromKey(key), storage), value);

    public static DataPage SetStorage(this DataPage page, in Keccak key, in Keccak storage, ReadOnlySpan<byte> data,
        IBatchContext batch)
    {
        return new DataPage(page.Set(new SetContext(Key.StorageCell(NibblePath.FromKey(key), storage), data, batch)));
    }

    public static DataPage SetAccount(this DataPage page, in Keccak key, ReadOnlySpan<byte> data, IBatchContext batch)
    {
        return new DataPage(page.Set(new SetContext(Key.Account(NibblePath.FromKey(key)), data, batch)));
    }

    public static void ShouldHaveAccount(this DataPage read, in Keccak key, ReadOnlySpan<byte> expected,
        IReadOnlyBatchContext batch)
    {
        read.TryGet(Key.Account(key), batch, out var value).Should().BeTrue();
        value.SequenceEqual(expected);
    }

    public static void ShouldHaveStorage(this DataPage read, in Keccak key, in Keccak storage, ReadOnlySpan<byte> expected,
        IReadOnlyBatchContext batch)
    {
        read.TryGet(Key.StorageCell(NibblePath.FromKey(key), storage), batch, out var value).Should().BeTrue();
        value.SequenceEqual(expected);
    }
}