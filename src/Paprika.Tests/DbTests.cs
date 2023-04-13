using System.Buffers.Binary;
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
    private const int MB64 = 128 * 1024 * 1024;

    [Test]
    public void Simple()
    {
        const int max = 2;

        using var db = new NativeMemoryPagedDb(1024 * 1024UL, 2);

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
            batch.Commit(CommitOptions.FlushDataOnly);
        }

        using var read = db.BeginNextBlock();

        for (byte i = 0; i < max; i++)
        {
            span[0] = (byte)(i << NibblePath.NibbleShift);
            var key = new Keccak(span);

            var account = read.GetAccount(key);

            Assert.AreEqual((UInt256)i, account.Nonce);
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

    [TestCase(1_000_000, 1, TestName = "Long history, single account")]
    [TestCase(50, 2_000, TestName = "Short history, many accounts")]
    public void Page_reuse(int blockCount, int accountsCount)
    {
        using var db = new NativeMemoryPagedDb(MB64, 2);

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

        Console.WriteLine($"Used {db.TotalUsedPages:P} pages for storing with {db.ActualMegabytesOnDisk:F2}MB on disk ");
    }
}