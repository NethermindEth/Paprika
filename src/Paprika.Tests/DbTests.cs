using System.Buffers.Binary;
using System.Diagnostics;
using FluentAssertions;
using Nethermind.Int256;
using NUnit.Framework;
using Paprika.Crypto;
using Paprika.Db;
using Paprika.Pages;
using static Paprika.Tests.Values;

namespace Paprika.Tests;

public class DbTests
{
    private const int SmallDb = 256 * Page.PageSize;
    private const int MB = 1024 * 1024;
    private const int MB16 = 16 * MB;
    private const int MB64 = 64 * MB;

    [Test]
    public void Simple()
    {
        const int max = 2;

        using var db = new NativeMemoryPagedDb(MB, 2);

        Span<byte> span = stackalloc byte[Keccak.Size];

        span[1] = 0x12;
        span[2] = 0x34;
        span[3] = 0x56;
        span[4] = 0x78;

        for (byte i = 0; i < max; i++)
        {
            span[0] = (byte)(i << NibblePath.NibbleShift);
            var key = new Keccak(span);

            using var batch = db.BeginNextBlock();
            batch.Set(key, new Account(i, i));
            batch.SetStorage(key, key, i);
            batch.Commit(CommitOptions.FlushDataAndRoot);
        }

        using var read = db.BeginNextBlock();

        for (byte i = 0; i < max; i++)
        {
            span[0] = (byte)(i << NibblePath.NibbleShift);
            var key = new Keccak(span);

            var expected = (UInt256)i;
            var account = read.GetAccount(key);
            Assert.AreEqual(expected, account.Nonce);

            var actual = read.GetStorage(key, key);
            actual.Should().Be(expected);
        }

        Console.WriteLine($"Used memory {db.TotalUsedPages:P}");
    }

    [Test]
    public void Reorganization_jump_to_given_block_hash()
    {
        using var db = new NativeMemoryPagedDb(SmallDb, 2);

        var account0 = new Account(Balance0, Nonce0);
        var account1 = new Account(Balance1, Nonce1);
        var account2 = new Account(Balance2, Nonce2);

        Keccak block0Commit;

        using (var block0 = db.BeginNextBlock())
        {
            block0.Set(Key0, account0);

            block0Commit = block0.Commit(CommitOptions.FlushDataOnly);
        }

        using (var block1A = db.BeginNextBlock())
        {
            block1A.Set(Key0, account1);
            block1A.Set(Key1a, account2);

            block1A.Commit(CommitOptions.FlushDataOnly);

            // assert
            block1A.GetAccount(Key0).Should().Be(account1);
            block1A.GetAccount(Key1a).Should().Be(account2);
        }

        using (var block1B = db.ReorganizeBackToAndStartNew(block0Commit))
        {
            block1B.GetAccount(Key0).Should().Be(account0);
            block1B.GetAccount(Key1a).Should().Be(Account.Empty);

            block1B.Set(Key0, account2);

            block1B.Commit(CommitOptions.FlushDataOnly);

            // assert
            block1B.GetAccount(Key0).Should().Be(account2);
            block1B.GetAccount(Key1a).Should().Be(Account.Empty);
        }
    }

    [Test]
    public void Reorganization_block_not_found()
    {
        using var db = new NativeMemoryPagedDb(SmallDb, 2);

        var account0 = new Account(Balance0, Nonce0);

        using (var block0 = db.BeginNextBlock())
        {
            block0.Set(Key0, account0);
            block0.Commit(CommitOptions.FlushDataOnly);
        }

        using (var block1A = db.BeginNextBlock())
        {
            block1A.Commit(CommitOptions.FlushDataOnly);
        }

        var invalidBlock = Keccak.EmptyTreeHash;

        Assert.Throws<ArgumentException>(() => db.ReorganizeBackToAndStartNew(invalidBlock).Should());
    }

    [TestCase(100_000, 1, TestName = "Long history, single account")]
    [TestCase(500, 2_000, TestName = "Short history, many accounts")]
    public void Page_reuse(int blockCount, int accountsCount)
    {
        const int size = MB64;

        using var db = new NativeMemoryPagedDb(size, 2, metrics =>
        {
            Debugger.Break();
        });

        for (var i = 0; i < blockCount; i++)
        {
            // ReSharper disable once ConvertToUsingDeclaration
            using (var block = db.BeginNextBlock())
            {
                for (var account = 0; account < accountsCount; account++)
                {
                    var key = Key0;

                    BinaryPrimitives.WriteInt32LittleEndian(key.BytesAsSpan, account);

                    block.Set(key, new Account(Balance0, (UInt256)i));
                }

                block.Commit(CommitOptions.FlushDataOnly);
            }
        }

        Console.WriteLine($"Uses {db.TotalUsedPages:P} pages out of pre-allocated {size / MB}MB od disk. This gives the actual {db.ActualMegabytesOnDisk:F2}MB on disk ");
    }

    [Test]
    public void Readonly_transaction_block_till_they_are_released()
    {
        const int size = MB16;
        const int blocksDuringReadAcquired = 500; // the number should be smaller than the number of buckets in the root
        const int blocksPostRead = 10_000;
        UInt256 start = 13;

        using var db = new NativeMemoryPagedDb(size, 2);

        // write first value
        using (var block = db.BeginNextBlock())
        {
            block.Set(Key0, new Account(Balance0, start));
            block.Commit(CommitOptions.FlushDataOnly);
        }

        // start read batch, it will make new allocs only
        var readBatch = db.BeginReadOnlyBatch();

        for (var i = 0; i < blocksDuringReadAcquired; i++)
        {
            // ReSharper disable once ConvertToUsingDeclaration
            using (var block = db.BeginNextBlock())
            {
                var value = start + (UInt256)i;

                // assert previous
                block.GetAccount(Key0).Nonce.Should().Be(value);

                block.Set(Key0, new Account(Balance0, value + 1));
                block.Commit(CommitOptions.FlushDataOnly);

                readBatch.GetAccount(Key0).Nonce.Should().Be(start);
            }
        }

        var snapshot = db.TotalUsedPages;

        // disable read
        readBatch.Dispose();

        // write again
        for (var i = 0; i < blocksPostRead; i++)
        {
            // ReSharper disable once ConvertToUsingDeclaration
            using (var block = db.BeginNextBlock())
            {
                var value = (UInt256)i + start;

                block.Set(Key0, new Account(Balance0, value + 1));
                block.Commit(CommitOptions.FlushDataOnly);
            }
        }

        db.TotalUsedPages.Should().Be(snapshot, "Database should not grow without read transaction active.");

        Console.WriteLine($"Uses {db.TotalUsedPages:P} pages out of pre-allocated {size / MB}MB od disk. This gives the actual {db.ActualMegabytesOnDisk:F2}MB on disk ");
    }

    [Test]
    public void State_and_storage()
    {
        const int size = MB16;
        using var db = new NativeMemoryPagedDb(size, 2);

        const int count = 100;

        using (var batch = db.BeginNextBlock())
        {
            for (uint i = 0; i < count; i++)
            {
                var address = GetStorageAddress(i);

                batch.Set(Key0, new Account(1, 1));
                batch.SetStorage(Key0, address, i);
            }

            batch.Commit(CommitOptions.FlushDataOnly);
        }

        using (var read = db.BeginReadOnlyBatch())
        {
            for (uint i = 0; i < count; i++)
            {
                var address = GetStorageAddress(i);
                read.GetStorage(Key0, address).Should().Be(i);
            }
        }

        static Keccak GetStorageAddress(uint i)
        {
            var address = Key1a;
            BinaryPrimitives.WriteUInt32LittleEndian(address.BytesAsSpan, i);
            return address;
        }
    }
}