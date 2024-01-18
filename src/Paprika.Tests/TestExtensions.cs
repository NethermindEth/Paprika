using System.Buffers.Binary;
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

    public static void ShouldHave(this IReadOnlyBatch read, in Keccak key, ReadOnlySpan<byte> expected)
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


    public static DataPage Set(this DataPage page, in Keccak key, in ReadOnlySpan<byte> data, IBatchContext batch)
    {
        return new DataPage(page.Set(NibblePath.FromKey(key.Span), data, batch));
    }

    public static DataPage Set(this DataPage page, in NibblePath path, in ReadOnlySpan<byte> data, IBatchContext batch)
    {
        return new DataPage(page.Set(path, data, batch));
    }

    public static DataPage GetAssert(this DataPage page, in NibblePath path, in ReadOnlySpan<byte> data, IBatchContext batch, int? ith = null)
    {
        var cause = $"{ith}th iteration";
        page.TryGet(path, batch, out var existing).Should().BeTrue(cause);
        existing.SequenceEqual(data).Should().BeTrue(cause);
        return page;
    }

    public static DataPage GetAssert(this DataPage page, in Keccak key, in ReadOnlySpan<byte> data, IBatchContext batch, int? ith = null)
    {
        var path = NibblePath.FromKey(key.Span);
        if (page.TryGet(path, batch, out var existing) == false)
        {
            false.Should().BeTrue($"{ith}th iteration for {path.ToString()} did not get data");
        }

        if (existing.SequenceEqual(data) == false)
        {
            false.Should().BeTrue($"{ith}th iteration for {path.ToString()} got WRONG data");
        }
        return page;
    }

    public static void ShouldHave(this DataPage read, in Keccak key, ReadOnlySpan<byte> expected,
        IReadOnlyBatchContext batch, int? iteration = null)
    {
        var because = $"Data for {key.Span.ToHexString(true)} should exist.";
        if (iteration != null)
        {
            because += $" Iteration: {iteration}";
        }
        read.TryGet(NibblePath.FromKey(key.Span), batch, out var value).Should().BeTrue(because);
        value.SequenceEqual(expected).Should()
            .BeTrue($"Expected value is {expected.ToHexString(false)} while actual is {value.ToHexString(false)}");
    }

    public static Keccak NextKeccak(this Random random)
    {
        Keccak keccak = default;
        random.NextBytes(keccak.BytesAsSpan);
        return keccak;
    }

    public static Task WaitTillFlush(this Blockchain chain, uint blockNumber)
    {
        var tcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        chain.Flushed += (_, block) =>
        {
            if (block.blockNumber == blockNumber)
                tcs.SetResult();
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