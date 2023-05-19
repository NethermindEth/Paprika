using System.Buffers.Binary;
using FluentAssertions;
using Nethermind.Int256;
using NUnit.Framework;
using Paprika.Crypto;
using Paprika.Data;
using Paprika.Store;
using static Paprika.Tests.Values;

namespace Paprika.Tests;

public class DbTests
{
    private const int SmallDb = 256 * Page.PageSize;
    private const int MB = 1024 * 1024;
    private const int MB16 = 16 * MB;
    private const int MB64 = 64 * MB;

    [Test]
    public async Task Simple()
    {
        const int max = 2;

        using var db = PagedDb.NativeMemoryDb(MB);

        byte[] span = new byte[Keccak.Size];

        span[1] = 0x12;
        span[2] = 0x34;
        span[3] = 0x56;
        span[4] = 0x78;

        for (byte i = 0; i < max; i++)
        {
            span[0] = (byte)(i << NibblePath.NibbleShift);
            var key = new Keccak(span);

            using var batch = db.BeginNextBatch();
            batch.Set(key, new Account(i, i));
            batch.SetStorage(key, key, i);
            await batch.Commit(CommitOptions.FlushDataAndRoot);
        }

        using var read = db.BeginNextBatch();

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

        Console.WriteLine($"Used memory {db.Megabytes:P}");
    }

    [TestCase(100_000, 1, TestName = "Long history, single account")]
    [TestCase(500, 2_000, TestName = "Short history, many accounts")]
    public async Task Page_reuse(int blockCount, int accountsCount)
    {
        const int size = MB64;

        using var db = PagedDb.NativeMemoryDb(size);

        for (var i = 0; i < blockCount; i++)
        {
            // ReSharper disable once ConvertToUsingDeclaration
            using (var block = db.BeginNextBatch())
            {
                for (var account = 0; account < accountsCount; account++)
                {
                    var key = Key0;

                    BinaryPrimitives.WriteInt32LittleEndian(key.BytesAsSpan, account);

                    block.Set(key, new Account(Balance0, (UInt256)i));
                }

                await block.Commit(CommitOptions.FlushDataOnly);
            }
        }

        Console.WriteLine($"Uses {db.Megabytes:P}MB out of pre-allocated {size / MB}MB od disk.");

        AssertPageMetadataAssigned(db);
    }

    [Test]
    public async Task Readonly_transaction_block_till_they_are_released()
    {
        const int size = MB16;
        const int blocksDuringReadAcquired = 500; // the number should be smaller than the number of buckets in the root
        const int blocksPostRead = 10_000;
        UInt256 start = 13;

        using var db = PagedDb.NativeMemoryDb(size);

        // write first value
        using (var block = db.BeginNextBatch())
        {
            block.Set(Key0, new Account(Balance0, start));
            await block.Commit(CommitOptions.FlushDataOnly);
        }

        // start read batch, it will make new allocs only
        var readBatch = db.BeginReadOnlyBatch();

        for (var i = 0; i < blocksDuringReadAcquired; i++)
        {
            // ReSharper disable once ConvertToUsingDeclaration
            using (var block = db.BeginNextBatch())
            {
                var value = start + (UInt256)i;

                // assert previous
                block.GetAccount(Key0).Nonce.Should().Be(value);

                block.Set(Key0, new Account(Balance0, value + 1));
                await block.Commit(CommitOptions.FlushDataOnly);

                readBatch.GetAccount(Key0).Nonce.Should().Be(start);
            }
        }

        var snapshot = db.Megabytes;

        // disable read
        readBatch.Dispose();

        // write again
        for (var i = 0; i < blocksPostRead; i++)
        {
            // ReSharper disable once ConvertToUsingDeclaration
            using (var block = db.BeginNextBatch())
            {
                var value = (UInt256)i + start;

                block.Set(Key0, new Account(Balance0, value + 1));
                await block.Commit(CommitOptions.FlushDataOnly);
            }
        }

        db.Megabytes.Should().Be(snapshot, "Database should not grow without read transaction active.");

        Console.WriteLine($"Uses {db.Megabytes:P}MB out of pre-allocated {size / MB}MB od disk.");

        AssertPageMetadataAssigned(db);
    }

    [Test]
    public async Task State_and_storage()
    {
        const int size = MB64;
        using var db = PagedDb.NativeMemoryDb(size);

        const int count = 100000;

        using (var batch = db.BeginNextBatch())
        {
            for (uint i = 0; i < count; i++)
            {
                var address = GetStorageAddress(i);

                batch.Set(Key0, new Account(1, 1));
                batch.SetStorage(Key0, address, i);
            }

            await batch.Commit(CommitOptions.FlushDataOnly);
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

        AssertPageMetadataAssigned(db);
    }

    private static void AssertPageMetadataAssigned(PagedDb db)
    {
        foreach (var page in db.UnsafeEnumerateNonRoot())
        {
            var header = page.Header;

            header.BatchId.Should().BeGreaterThan(0);
            header.PageType.Should().BeOneOf(PageType.Abandoned, PageType.Standard, PageType.MassiveStorageTree);
            if (header.PageType is PageType.MassiveStorageTree or PageType.Abandoned)
            {
                header.TreeLevel.Should().BeGreaterOrEqualTo(0);
            }
            else
            {
                // any non-root data should be bigger than 0
                header.TreeLevel.Should().BeGreaterThan(0);
            }

            header.PaprikaVersion.Should().Be(1);
        }
    }
}