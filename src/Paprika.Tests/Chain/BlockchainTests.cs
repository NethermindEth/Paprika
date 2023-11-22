using System.Buffers.Binary;
using System.Runtime.CompilerServices;
using System.Text;
using FluentAssertions;
using Nethermind.Int256;
using NUnit.Framework;
using Paprika.Chain;
using Paprika.Crypto;
using Paprika.Merkle;
using Paprika.Store;
using static Paprika.Tests.Values;

namespace Paprika.Tests.Chain;

public class BlockchainTests
{
    private const int Mb = 1024 * 1024;

    [Test]
    public async Task Simple()
    {
        var account1A = new Account(1, 1);
        var account1B = new Account(2, 2);

        using var db = PagedDb.NativeMemoryDb(16 * Mb, 2);

        await using var blockchain = new Blockchain(db, new PreCommit());

        Keccak keccak2A;

        using (var block1A = blockchain.StartNew(Keccak.EmptyTreeHash))
        using (var block1B = blockchain.StartNew(Keccak.EmptyTreeHash))
        {
            block1A.SetAccount(Key0, account1A);
            block1B.SetAccount(Key0, account1B);

            block1A.GetAccount(Key0).Should().Be(account1A);
            block1B.GetAccount(Key0).Should().Be(account1B);

            // commit both blocks as they were seen in the network
            var keccak1A = block1A.Commit(1);
            block1B.Commit(1);

            // start a next block
            using var block2A = blockchain.StartNew(keccak1A);

            // set some dummy value
            block2A.SetAccount(Key1, account1B);

            // assert whether the history is preserved
            block2A.GetAccount(Key0).Should().Be(account1A);
            keccak2A = block2A.Commit(2);
        }

        // finalize second block
        blockchain.Finalize(keccak2A);

        // for now, to monitor the block chain, requires better handling of ref-counting on finalized
        await Task.Delay(1000);

        // start the third block
        using var block3A = blockchain.StartNew(keccak2A);

        block3A.GetAccount(Key0).Should().Be(account1A);
    }

    [Test(Description =
        "Not finalize the last block but one but last to see that dependencies are properly propagated.")]
    public async Task Finalization_queue()
    {
        const int count = 1000;
        const int lastValue = count - 1;

        using var db = PagedDb.NativeMemoryDb(16 * Mb, 2);

        await using var blockchain = new Blockchain(db, new ComputeMerkleBehavior(true, 2, 2));

        var block = blockchain.StartNew(Keccak.EmptyTreeHash);
        block.SetAccount(Key0, new Account(1, 1));
        var hash = block.Commit(1);

        block.Dispose();

        for (uint no = 2; no < count; no++)
        {
            // create new, set, commit and dispose
            block = blockchain.StartNew(hash);
            block.SetAccount(Key0, new Account(no, no));

            // finalize but only previous so that the dependency is there and should be managed properly
            blockchain.Finalize(hash);

            hash = block.Commit(no);
            block.Dispose();
        }

        // DO NOT FINALIZE the last block! it will clean the dependencies and destroy the purpose of the test
        // blockchain.Finalize(block.Hash);

        // for now, to monitor the block chain, requires better handling of ref-counting on finalized
        await Task.Delay(1000);

        using var last = blockchain.StartNew(hash);
        last.GetAccount(Key0).Should().Be(new Account(lastValue, lastValue));
    }

    [Test]
    public async Task Account_destruction_same_block()
    {
        using var db = PagedDb.NativeMemoryDb(1 * Mb, 2);
        await using var blockchain = new Blockchain(db, new ComputeMerkleBehavior());

        using var block = blockchain.StartNew(Keccak.EmptyTreeHash);

        var before = block.Hash;

        block.SetAccount(Key0, new Account(1, 1));
        block.SetStorage(Key0, Key1, stackalloc byte[1] { 1 });

        // force hash calculation
        var mid = block.Hash;

        block.DestroyAccount(Key0);
        block.GetAccount(Key0).Should().Be(new Account(0, 0));
        block.AssertNoStorageAt(Key0, Key1);

        var after = block.Hash;

        before.Should().Be(after);
        before.Should().NotBe(mid);
    }

    [Test]
    public async Task Account_destruction_multi_block()
    {
        using var db = PagedDb.NativeMemoryDb(1 * Mb, 2);
        await using var blockchain = new Blockchain(db, new ComputeMerkleBehavior());

        using var block1 = blockchain.StartNew(Keccak.EmptyTreeHash);

        var before = block1.Hash;

        block1.SetAccount(Key0, new Account(1, 1));
        block1.SetStorage(Key0, Key1, stackalloc byte[1] { 1 });

        var hash = block1.Commit(1);

        var mid = hash;

        using var block2 = blockchain.StartNew(hash);

        block2.DestroyAccount(Key0);

        block2.GetAccount(Key0).Should().Be(new Account(0, 0));
        block2.AssertNoStorageAt(Key0, Key1);

        var after = block2.Hash;

        before.Should().Be(after);
        before.Should().NotBe(mid);
    }

    [Test]
    public async Task Account_destruction_database_flushed()
    {
        uint blockNo = 1;

        using var db = PagedDb.NativeMemoryDb(1 * Mb, 2);
        await using var blockchain = new Blockchain(db, new ComputeMerkleBehavior());

        using var block1 = blockchain.StartNew(Keccak.EmptyTreeHash);

        var before = block1.Hash;
        
        block1.SetAccount(Key0, new Account(1, 1));
        block1.SetStorage(Key0, Key1, stackalloc byte[1] { 1 });

        var hash = block1.Commit(blockNo++);

        blockchain.Finalize(hash);

        // Poor man's await on finalization flushed
        await Task.Delay(500);

        using var block2 = blockchain.StartNew(hash);

        block2.DestroyAccount(Key0);
        var hash2 = block2.Commit(blockNo);

        var wait = blockchain.WaitTillFlush(blockNo);

        blockchain.Finalize(hash2);

        await wait;

        using var read = db.BeginReadOnlyBatch();

        read.Metadata.BlockNumber.Should().Be(2);

        read.AssertNoAccount(Key0);
        read.AssertNoStorageAt(Key0, Key1);

        hash2.Should().Be(before);
    }

    [Test]
    public async Task BiggerTest()
    {
        const int blockCount = 10;
        const int perBlock = 1_000;

        using var db = PagedDb.NativeMemoryDb(256 * Mb, 2);
        var counter = 0;

        var behavior = new ComputeMerkleBehavior(true, 2, 2);

        await using (var blockchain = new Blockchain(db, behavior))
        {
            var hash = Keccak.Zero;

            for (uint i = 1; i < blockCount + 1; i++)
            {
                using var block = blockchain.StartNew(hash);

                for (var j = 0; j < perBlock; j++)
                {
                    var key = BuildKey(counter);

                    block.SetAccount(key, GetAccount(counter));
                    block.SetStorage(key, key, ((UInt256)counter).ToBigEndian());

                    counter++;
                }

                // commit first
                hash = block.Commit(i);

                if (i > 1)
                {
                    blockchain.Finalize(hash);
                }
            }
        }

        using var read = db.BeginReadOnlyBatch();

        read.Metadata.BlockNumber.Should().Be(blockCount);

        // reset the counter
        counter = 0;
        for (int i = 1; i < blockCount + 1; i++)
        {
            for (var j = 0; j < perBlock; j++)
            {
                var key = BuildKey(counter);

                read.ShouldHaveAccount(key, GetAccount(counter), true);
                read.AssertStorageValue(key, key, ((UInt256)counter).ToBigEndian());

                counter++;
            }
        }
    }

    [Test]
    public async Task Start_in_the_past()
    {
        var account1 = new Account(1, 1);
        var account2 = new Account(2, 2);

        using var db = PagedDb.NativeMemoryDb(1 * Mb, 4);

        await using var blockchain = new Blockchain(db, new PreCommit());

        using (var block1A = blockchain.StartNew(Keccak.EmptyTreeHash))
        {
            block1A.SetAccount(Key0, account1);
            block1A.GetAccount(Key0).Should().Be(account1);
            var keccak1A = block1A.Commit(1);

            using (var block2A = blockchain.StartNew(keccak1A))
            {
                block2A.SetAccount(Key0, account2);
                const int block2 = 2;

                var keccak2A = block2A.Commit(block2);
                var task = blockchain.WaitTillFlush(block2);

                blockchain.Finalize(keccak2A);

                await task;

                // start in the past
                using (var block2B = blockchain.StartNew(keccak1A))
                {
                    block2B.GetAccount(Key0).Should().Be(account1);
                }
            }
        }
    }

    private static Account GetAccount(int i) => new((UInt256)i, (UInt256)i);

    private static Keccak BuildKey(int i)
    {
        Span<byte> span = stackalloc byte[4];
        BinaryPrimitives.WriteInt32LittleEndian(span, i);
        return Keccak.Compute(span);
    }

    private class PreCommit : IPreCommitBehavior
    {
        public Keccak BeforeCommit(ICommit commit)
        {
            var hashCode = RuntimeHelpers.GetHashCode(commit);
            Keccak hash = default;
            BinaryPrimitives.WriteInt32LittleEndian(hash.BytesAsSpan, hashCode);
            return hash;
        }
    }
}

file static class BlockExtensions
{
    public static void AssertNoStorageAt(this IWorldState state, in Keccak address, in Keccak storage)
    {
        state.GetStorage(address, storage, stackalloc byte[32]).IsEmpty.Should().BeTrue();
    }
}