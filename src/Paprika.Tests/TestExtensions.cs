﻿using System.Buffers.Binary;
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
    public static byte[] ToByteArray(this int value)
    {
        byte[] bytes = new byte[sizeof(int)];
        BinaryPrimitives.WriteInt32BigEndian(bytes, value);
        return bytes;
    }

    public static void ShouldHaveAccount(this IReadOnlyBatch read, in Keccak key, ReadOnlySpan<byte> expected)
    {
        read.TryGet(Key.Account(key), out var value).Should().BeTrue();
        value.SequenceEqual(expected);
    }

    public static void ShouldHaveAccount(this IReadOnlyBatch read, in Keccak key, in Account expected)
    {
        var raw = expected.WriteTo(stackalloc byte[Account.MaxByteCount]);

        read.TryGet(Key.Account(key), out var value).Should().BeTrue();
        value.SequenceEqual(raw).Should().BeTrue();
    }

    public static Account GetAccount(this IReadOnlyBatch read, in Keccak key)
    {
        read.TryGet(Key.Account(key), out var value).Should().BeTrue($"Key: {key.ToString()} should exist.");
        Account.ReadFrom(value, out var account);
        return account;
    }

    public static byte[] GetStorage(this IReadOnlyBatch read, in Keccak key, in Keccak storage)
    {
        read.TryGet(Key.StorageCell(NibblePath.FromKey(key), storage), out var value).Should()
            .BeTrue($"Storage for the account: {key.ToString()} @ {storage.ToString()} should exist.");
        return value.ToArray();
    }

    public static void ShouldHaveStorage(this IReadOnlyBatch read, in Keccak key, in Keccak storage,
        ReadOnlySpan<byte> expected)
    {
        read.TryGet(Key.StorageCell(NibblePath.FromKey(key), storage), out var value).Should().BeTrue();
        value.SequenceEqual(expected);
    }

    public static void SetAccount(this IBatch batch, in Keccak key, ReadOnlySpan<byte> value) =>
        batch.SetRaw(Key.Account(key), value);

    public static void SetStorage(this IBatch batch, in Keccak key, in Keccak storage, ReadOnlySpan<byte> value) =>
        batch.SetRaw(Key.StorageCell(NibblePath.FromKey(key), storage), value);

    public static DataPage SetStorage(this DataPage page, in Keccak key, in Keccak storage, ReadOnlySpan<byte> data,
        IBatchContext batch)
    {
        var k = Key.StorageCell(NibblePath.FromKey(key), storage);
        var hash = HashingMap.GetHash(k);
        return new DataPage(page.Set(new SetContext(hash, k, data, batch)));
    }

    public static DataPage SetAccount(this DataPage page, in Keccak key, ReadOnlySpan<byte> data, IBatchContext batch)
    {
        var k = Key.Account(NibblePath.FromKey(key));
        var hash = HashingMap.GetHash(k);
        return new DataPage(page.Set(new SetContext(hash, k, data, batch)));
    }

    public static void ShouldHaveAccount(this DataPage read, in Keccak key, ReadOnlySpan<byte> expected,
        IReadOnlyBatchContext batch, int? iteration = null)
    {
        var account = Key.Account(key);
        var hash = HashingMap.GetHash(account);
        var because = $"Data for {account.Path.ToString()} should exist.";
        if (iteration != null)
        {
            because += $" Iteration: {iteration}";
        }
        read.TryGet(hash, account, batch, out var value).Should().BeTrue(because);
        value.SequenceEqual(expected).Should()
            .BeTrue($"Expected value is {expected.ToHexString(false)} while actual is {value.ToHexString(false)}");
    }

    public static void ShouldHaveStorage(this DataPage read, in Keccak key, in Keccak storage, ReadOnlySpan<byte> expected,
        IReadOnlyBatchContext batch)
    {
        var storageCell = Key.StorageCell(NibblePath.FromKey(key), storage);
        var hash = HashingMap.GetHash(storageCell);
        var because = $"Storage at {storageCell.ToString()} should exist";
        read.TryGet(hash, storageCell, batch, out var value).Should().BeTrue(because);
        value.SequenceEqual(expected).Should().BeTrue();
    }

    public static Keccak NextKeccak(this Random random)
    {
        Keccak keccak = default;
        random.NextBytes(keccak.BytesAsSpan);
        return keccak;
    }
}