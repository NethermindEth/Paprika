using System.Buffers.Binary;
using FluentAssertions;
using Nethermind.Int256;
using NUnit.Framework;
using Paprika.Crypto;
using Paprika.Store;
using static Paprika.Tests.Values;

namespace Paprika.Tests.Store;

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

        for (byte i = 0; i < max; i++)
        {
            var key = GetKey(i);

            using var batch = db.BeginNextBatch();

            var value = GetValue(i);

            batch.SetAccount(key, value);
            batch.SetStorage(key, key, value);

            await batch.Commit(CommitOptions.FlushDataAndRoot);
        }

        using var read = db.BeginNextBatch();

        Assert(read);

        Console.WriteLine($"Used memory {db.Megabytes:P}");

        static void Assert(IReadOnlyBatch read)
        {
            for (byte i = 0; i < max; i++)
            {
                var key = GetKey(i);
                var expected = GetValue(i);

                read.ShouldHaveAccount(key, expected);
                read.ShouldHaveStorage(key, key, expected);
            }
        }
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
            using (var batch = db.BeginNextBatch())
            {
                for (var account = 0; account < accountsCount; account++)
                {
                    batch.SetAccount(GetKey(i), GetValue(i));
                }

                await batch.Commit(CommitOptions.FlushDataOnly);
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
        const int start = -1;

        using var db = PagedDb.NativeMemoryDb(size);

        // write first value
        using (var block = db.BeginNextBatch())
        {
            block.SetAccount(Key0, GetValue(start));
            await block.Commit(CommitOptions.FlushDataOnly);
        }

        // start read batch, it will make new allocs only
        var read = db.BeginReadOnlyBatch();

        for (var i = 0; i < blocksDuringReadAcquired; i++)
        {
            // ReSharper disable once ConvertToUsingDeclaration
            using (var block = db.BeginNextBatch())
            {
                // assert previous
                block.ShouldHaveAccount(Key0, GetValue(i + start));

                // write current
                block.SetAccount(Key0, GetValue(i));
                await block.Commit(CommitOptions.FlushDataOnly);

                read.ShouldHaveAccount(Key0, GetValue(start));
            }
        }

        var snapshot = db.Megabytes;

        // disable read
        read.Dispose();

        // write again
        for (var i = 0; i < blocksPostRead; i++)
        {
            // ReSharper disable once ConvertToUsingDeclaration
            using (var block = db.BeginNextBatch())
            {
                var value = GetValue(i + start);

                block.SetAccount(Key0, value);
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
            for (int i = 0; i < count; i++)
            {
                var address = GetStorageAddress(i);

                batch.SetAccount(Key0, GetValue(i));
                batch.SetStorage(Key0, address, GetValue(i));
            }

            await batch.Commit(CommitOptions.FlushDataOnly);
        }

        using (var read = db.BeginReadOnlyBatch())
        {
            for (int i = 0; i < count; i++)
            {
                var address = GetStorageAddress(i);
                read.ShouldHaveStorage(Key0, address, GetValue(i));
            }
        }

        static Keccak GetStorageAddress(int i)
        {
            var address = Key1A;
            BinaryPrimitives.WriteInt32LittleEndian(address.BytesAsSpan, i);
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
            header.PageType.Should().BeOneOf(PageType.Abandoned, PageType.Standard, PageType.PrefixPage);
            header.PaprikaVersion.Should().Be(1);
        }
    }

    private static Keccak GetKey(int i)
    {
        var keccak = Keccak.EmptyTreeHash;
        BinaryPrimitives.WriteInt32LittleEndian(keccak.BytesAsSpan, i);
        return keccak;
    }

    static byte[] GetValue(int i) => new UInt256((uint)i).ToBigEndian();
}