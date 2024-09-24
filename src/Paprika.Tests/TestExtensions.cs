using System.Buffers.Binary;
using System.Security.Cryptography;
using FluentAssertions;
using NUnit.Framework;
using Paprika.Chain;
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

    public static void ShouldHaveAccount(this IReadOnlyBatch read, in Keccak key, in Account expected,
        bool skipStorageRootCheck = false)
    {
        read.TryGet(Key.Account(key), out var value).Should().BeTrue();

        if (skipStorageRootCheck)
        {
            Account.ReadFrom(value, out var actual);
            actual.Balance.Should().Be(expected.Balance);
            actual.Nonce.Should().Be(expected.Nonce);
            actual.CodeHash.Should().Be(expected.CodeHash);
        }
        else
        {
            var raw = expected.WriteTo(stackalloc byte[Account.MaxByteCount]);
            value.SequenceEqual(raw).Should().BeTrue();
        }
    }

    public static void AssertNoAccount(this IReadOnlyBatch read, in Keccak key)
    {
        read.TryGet(Key.Account(key), out _).Should().BeFalse();
    }

    public static Account GetAccount(this IReadOnlyBatch read, in Keccak key)
    {
        if (!read.TryGet(Key.Account(key), out var value))
        {
            Assert.Fail($"Key: {key.ToString()} should exist.");
        }

        Account.ReadFrom(value, out var account);
        return account;
    }

    public static void AssertStorageValue(this IReadOnlyBatch read, in Keccak key, in Keccak storage,
        ReadOnlySpan<byte> expected)
    {
        if (!read.TryGet(Key.StorageCell(NibblePath.FromKey(key), storage), out var value))
        {
            Assert.Fail($"Storage for the account: {key.ToString()} @ {storage.ToString()} should exist.");
        }

        if (value.SequenceEqual(expected) == false)
        {
            throw new InvalidOperationException($"Invalid storage value for account number {key.ToString()} @ {storage.ToString()}! " +
                                                $"Expected was '{expected.ToHexString(false)}' while actual '{value.ToHexString(false)}'");
        }
    }

    public static void AssertNoStorageAt(this IReadOnlyBatch read, in Keccak key, in Keccak storage)
    {
        read.TryGet(Key.StorageCell(NibblePath.FromKey(key), storage), out var value).Should().BeFalse();
    }

    public static void SetAccount(this IBatch batch, in Keccak key, ReadOnlySpan<byte> value) =>
        batch.SetRaw(Key.Account(key), value);

    public static void SetStorage(this IBatch batch, in Keccak key, in Keccak storage, ReadOnlySpan<byte> value) =>
        batch.SetRaw(Key.StorageCell(NibblePath.FromKey(key), storage), value);

    // public static DataPage SetStorage(this DataPage page, in Keccak key, in Keccak storage, ReadOnlySpan<byte> data,
    //     IBatchContext batch)
    // {
    //     var k = Key.StorageCell(NibblePath.FromKey(key), storage);
    //     return new DataPage(page.Set(new SetContext(k, data, batch)));
    // }
    //
    // public static DataPage SetAccount(this DataPage page, in Keccak key, ReadOnlySpan<byte> data, IBatchContext batch)
    // {
    //     return new DataPage(page.Set(new SetContext(NibblePath.FromKey(key), data, batch)));
    // }
    //
    // public static DataPage SetMerkle(this DataPage page, in Keccak key, ReadOnlySpan<byte> data, IBatchContext batch)
    // {
    //     var k = Key.Merkle(NibblePath.FromKey(key));
    //     return new DataPage(page.Set(new SetContext(k, data, batch)));
    // }
    //
    // public static DataPage SetMerkle(this DataPage page, in Keccak key, in NibblePath storagePath, ReadOnlySpan<byte> data, IBatchContext batch)
    // {
    //     var k = Key.Raw(NibblePath.FromKey(key), DataType.Merkle, storagePath);
    //     return new DataPage(page.Set(new SetContext(k, data, batch)));
    // }
    //
    // public static void ShouldHaveMerkle(this DataPage read, in Keccak key, ReadOnlySpan<byte> expected,
    //     IReadOnlyBatchContext batch, int? iteration = null)
    // {
    //     var account = Key.Merkle(NibblePath.FromKey(key));
    //     var because = $"Merkle for {account.Path.ToString()} should exist.";
    //     if (iteration != null)
    //     {
    //         because += $" Iteration: {iteration}";
    //     }
    //
    //     read.TryGet(account, batch, out var value).Should().BeTrue(because);
    //     value.SequenceEqual(expected).Should()
    //         .BeTrue($"Expected value is {expected.ToHexString(false)} while actual is {value.ToHexString(false)}");
    // }
    //
    // public static void ShouldHaveMerkle(this DataPage read, in Keccak key, NibblePath storagePath, ReadOnlySpan<byte> expected,
    //     IReadOnlyBatchContext batch, int? iteration = null)
    // {
    //     var k = Key.Raw(NibblePath.FromKey(key), DataType.Merkle, storagePath);
    //     var because = $"Merkle for {k.ToString()} should exist.";
    //     if (iteration != null)
    //     {
    //         because += $" Iteration: {iteration}";
    //     }
    //
    //     read.TryGet(k, batch, out var value).Should().BeTrue(because);
    //     value.SequenceEqual(expected).Should()
    //         .BeTrue($"Expected value is {expected.ToHexString(false)} while actual is {value.ToHexString(false)}");
    // }
    //
    // public static void ShouldHaveAccount(this DataPage read, in Keccak key, ReadOnlySpan<byte> expected,
    //     IReadOnlyBatchContext batch, int? iteration = null)
    // {
    //     var account = Key.Account(key);
    //     var because = $"Data for {account.Path.ToString()} should exist.";
    //     if (iteration != null)
    //     {
    //         because += $" Iteration: {iteration}";
    //     }
    //     read.TryGet(account, batch, out var value).Should().BeTrue(because);
    //     value.SequenceEqual(expected).Should()
    //         .BeTrue($"Expected value is {expected.ToHexString(false)} while actual is {value.ToHexString(false)}");
    // }
    //
    // public static void ShouldHaveStorage(this DataPage read, in Keccak key, in Keccak storage, ReadOnlySpan<byte> expected,
    //     IReadOnlyBatchContext batch)
    // {
    //     var storageCell = Key.StorageCell(NibblePath.FromKey(key), storage);
    //     var because = $"Storage at {storageCell.ToString()} should exist";
    //     read.TryGet(storageCell, batch, out var value).Should().BeTrue(because);
    //     value.SequenceEqual(expected).Should().BeTrue();
    // }

    public static Keccak NextKeccak(this Random random)
    {
        Keccak keccak = default;
        random.NextBytes(keccak.BytesAsSpan);
        return keccak;
    }

    public static byte NextByte(this Random random) => (byte)random.Next(0, byte.MaxValue);

    public static Task WaitTillFlush(this Blockchain chain, uint blockNumber)
    {
        var tcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        chain.Flushed += (_, block) =>
        {
            if (block.blockNumber >= blockNumber)
                tcs.TrySetResult();
        };

        chain.FlusherFailure += (_, ex) =>
        {
            tcs.TrySetException(ex);
        };

        return tcs.Task;
    }

    public static Task WaitTillFlush(this Blockchain chain, Keccak hash)
    {
        var tcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        chain.Flushed += (_, block) =>
        {
            if (block.blockHash == hash)
                tcs.SetResult();
        };

        return tcs.Task;
    }
}
