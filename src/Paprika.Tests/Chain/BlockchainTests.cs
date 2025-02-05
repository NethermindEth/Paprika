using System.Buffers.Binary;
using System.Diagnostics.Metrics;
using System.Runtime.CompilerServices;
using FluentAssertions;
using Nethermind.Int256;
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

    [Test]
    public async Task Storage_setter()
    {
        var account1 = new Account(1, 1);
        var account2 = new Account(2, 2);

        using var db = PagedDb.NativeMemoryDb(1 * Mb, 2);

        await using var blockchain = new Blockchain(db, new ComputeMerkleBehavior());

        var storageKey1 = Key1;
        byte[] storageValue1 = [1];

        var storageKey2 = Key2;
        byte[] storageValue2 = [7];

        using var block1 = blockchain.StartNew(Keccak.EmptyTreeHash);

        // account Key0
        block1.SetAccount(Key0, account1);
        block1.SetStorage(Key0, storageKey1, storageValue1);
        block1.SetStorage(Key0, storageKey2, storageValue2);

        // account Key1 using StorageSetter
        block1.SetAccount(Key1, account2);
        var setter = block1.GetStorageSetter(Key1);
        setter.SetStorage(storageKey1, storageValue1);
        setter.SetStorage(storageKey2, storageValue2);

        var keccak1 = block1.Commit(1);

        // Assert that the roots are the same.
        using var reader = blockchain.StartReadOnly(keccak1);

        var read1 = reader.GetAccount(Key0);
        read1.Balance.Should().Be(account1.Balance);
        read1.Nonce.Should().Be(account1.Nonce);

        var read2 = reader.GetAccount(Key1);
        read2.Balance.Should().Be(account2.Balance);
        read2.Nonce.Should().Be(account2.Nonce);

        read1.StorageRootHash.Should().Be(read2.StorageRootHash);
    }

    [Test]
    [Explicit("Non parallel")]
    public async Task Delays_reporting_metrics()
    {
        using var listener = new MeterListener();
        listener.InstrumentPublished = (instrument, l) => { l.EnableMeasurementEvents(instrument, instrument); };
        listener.MeasurementsCompleted = (instrument, l) => { listener.DisableMeasurementEvents(instrument); };

        const int spins = 1000;

        var allow = new ManualResetEventSlim(false);

        listener.Start();

        listener.SetMeasurementEventCallback<double>((i, m, l, c) => Notify());
        listener.SetMeasurementEventCallback<float>((i, m, l, c) => Notify());
        listener.SetMeasurementEventCallback<long>((i, m, l, c) => Notify());
        listener.SetMeasurementEventCallback<int>((i, m, l, c) => Notify());
        listener.SetMeasurementEventCallback<short>((i, m, l, c) => Notify());
        listener.SetMeasurementEventCallback<byte>((i, m, l, c) => Notify());
        listener.SetMeasurementEventCallback<decimal>((i, m, l, c) => Notify());

        using var db = PagedDb.NativeMemoryDb(1 * Mb, 2);

        allow.Set();
        await using var blockchain = new Blockchain(db, new PreCommit());
        allow.Reset();

        var block = blockchain.StartNew(Keccak.EmptyTreeHash);
        block.SetAccount(Key0, new Account(1, 1));

        for (var i = 0; i < spins; i++)
        {
            Keccak k = default;
            BinaryPrimitives.WriteInt32LittleEndian(k.BytesAsSpan, i);
            block.GetAccount(k).Should().Be(new Account(0, 0));
        }

        allow.Set();
        block.Dispose();
        allow.Reset();

        void Notify()
        {
            allow.IsSet.Should().BeTrue();
        }
    }

    [Test(Description =
        "Not finalize the last block but one but last to see that dependencies are properly propagated.")]
    public async Task Finalization_queue()
    {
        const int count = 1000;
        const int lastValue = count - 1;

        using var db = PagedDb.NativeMemoryDb(16 * Mb, 2);

        await using var blockchain = new Blockchain(db, new ComputeMerkleBehavior());

        var block = blockchain.StartNew(Keccak.EmptyTreeHash);
        block.SetAccount(Key0, new Account(1, 1));
        var hash = block.Commit(1);

        block.Dispose();

        Task finality = Task.CompletedTask;

        for (uint no = 2; no < count; no++)
        {
            // create new, set, commit and dispose
            block = blockchain.StartNew(hash);
            block.SetAccount(Key0, new Account(no, no));

            // Finalize but only previous so that the dependency is there and should be managed properly
            finality = blockchain.Finalize(hash);

            hash = block.Commit(no);
            block.Dispose();
        }

        // DO NOT FINALIZE the last block! it will clean the dependencies and destroy the purpose of the test
        // blockchain.Finalize(block.Hash);

        // Await the last to be finalized.
        await finality;

        using var last = blockchain.StartNew(hash);
        last.GetAccount(Key0).Should().Be(new Account(lastValue, lastValue));
    }

    // [Test]
    // public async Task Automatic_finality()
    // {
    //     const int count = 100;
    //     const int automaticFinalityAfter = 10;
    //
    //     using var db = PagedDb.NativeMemoryDb(16 * Mb, 2);
    //
    //     await using var blockchain = new Blockchain(db, new ComputeMerkleBehavior(),
    //         null, default, default, null, automaticFinalityAfter);
    //
    //     var hashes = new Queue<Keccak>();
    //
    //     var block = blockchain.StartNew(Keccak.EmptyTreeHash);
    //     block.SetAccount(Key0, new Account(1, 1));
    //     var hash = block.Commit(1);
    //     hashes.Enqueue(hash);
    //
    //     block.Dispose();
    //
    //     var finalized = Task.CompletedTask;
    //
    //     for (uint no = 2; no < count; no++)
    //     {
    //         // create new, set, commit and dispose
    //         block = blockchain.StartNew(hash);
    //         block.SetAccount(Key0, new Account(no, no));
    //
    //         if (no > automaticFinalityAfter)
    //         {
    //             finalized = blockchain.WaitTillFlush(hashes.Dequeue());
    //             finalized.IsCompleted.Should().BeFalse("The automatic finality should be reached only on the commit");
    //         }
    //
    //         hash = block.Commit(no);
    //         hashes.Enqueue(hash);
    //
    //         // Should be finalized after the block breaching the finality is committed
    //         await finalized;
    //
    //         block.Dispose();
    //     }
    // }

    [Test]
    public async Task Account_destruction_same_block()
    {
        using var db = PagedDb.NativeMemoryDb(1 * Mb, 2);
        await using var blockchain = new Blockchain(db, new ComputeMerkleBehavior());

        var before = Keccak.EmptyTreeHash;

        using var worldState = blockchain.StartNew(before);

        worldState.SetAccount(Key0, new Account(1, 1));
        worldState.SetStorage(Key0, Key1, stackalloc byte[1] { 1 });

        // force hash calculation
        var mid = worldState.Hash;

        worldState.DestroyAccount(Key0);
        worldState.GetAccount(Key0).Should().Be(new Account(0, 0));
        worldState.AssertNoStorageAt(Key0, Key1);

        var after = worldState.Hash;

        before.Should().Be(after);
        before.Should().NotBe(mid);
    }

    [Test]
    [Category(Categories.LongRunning)]
    public async Task Account_destruction_spin()
    {
        using var db = PagedDb.NativeMemoryDb(8 * Mb, 2);
        await using var blockchain = new Blockchain(db, new ComputeMerkleBehavior());

        var parent = Keccak.EmptyTreeHash;

        var finality = new Queue<Keccak>();

        byte[] value = [13];
        byte[] read = new byte[32];

        const uint spins = 200;
        for (uint at = 1; at < spins; at++)
        {
            using var block = blockchain.StartNew(parent);

            // 10 deletes per block
            for (int i = 0; i < 10; i++)
            {
                Keccak k = default;
                BinaryPrimitives.WriteInt32LittleEndian(k.BytesAsSpan, i);
                block.DestroyAccount(k);
            }

            // 10 sets of various values
            for (int sets = 0; sets < 10; sets++)
            {
                Keccak k = default;
                BinaryPrimitives.WriteInt32LittleEndian(k.BytesAsSpan, sets + 1000_000);
                block.SetAccount(k, new Account(at, at));
                block.SetStorage(k, Key1, value);
            }

            // destroy this one
            block.DestroyAccount(Key0);

            // set account Key2
            block.SetAccount(Key2, new Account(at, at));

            // read non-existing entries for Key2
            const int readsPerSpin = 1000;
            for (var readCount = 0; readCount < readsPerSpin; readCount++)
            {
                var unique = at * readsPerSpin + readCount;
                // read values with unique keys, so that they are not cached
                Keccak k = default;
                BinaryPrimitives.WriteInt64LittleEndian(k.BytesAsSpan, unique);
                block.GetStorage(Key2, k, read);
            }

            parent = block.Commit(at + 1);
            finality.Enqueue(parent);

            if (finality.Count > 64)
            {
                await blockchain.Finalize(finality.Dequeue());
            }
        }

        while (finality.TryDequeue(out var finalized))
        {
            await blockchain.Finalize(finalized);
        }
    }

    [Test]
    public async Task Account_destruction_multi_block()
    {
        using var db = PagedDb.NativeMemoryDb(1 * Mb, 2);
        await using var blockchain = new Blockchain(db, new ComputeMerkleBehavior());

        var before = Keccak.EmptyTreeHash;
        using var block1 = blockchain.StartNew(before);

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
    public async Task Account_destruction_multi_block_2()
    {
        using var db = PagedDb.NativeMemoryDb(1 * Mb, 2);
        await using var blockchain = new Blockchain(db, new ComputeMerkleBehavior());

        using var block1 = blockchain.StartNew(Keccak.EmptyTreeHash);

        var before = block1.Hash;

        block1.SetAccount(Key0, new Account(1, 1));
        block1.SetStorage(Key0, Key1, stackalloc byte[1] { 1 });

        var hash1 = block1.Commit(1);

        var mid = hash1;

        using var block2 = blockchain.StartNew(hash1);

        // destroy previous
        block2.DestroyAccount(Key0);
        block2.SetAccount(Key1, new Account(2, 2));

        var hash2 = block2.Commit(2);

        using var block3 = blockchain.StartNew(hash2);

        const string reason = "Destroying an account should be true across blocks";
        block3.GetStorage(Key0, Key1, stackalloc byte[32])
            .IsEmpty
            .Should()
            .BeTrue(reason);

        block3.GetAccount(Key0)
            .Should()
            .Be(new Account(0, 0), reason);
    }

    [Test]
    public async Task Account_destruction_multi_block_3()
    {
        using var db = PagedDb.NativeMemoryDb(1 * Mb, 2);
        await using var blockchain = new Blockchain(db, new ComputeMerkleBehavior());

        using var block1 = blockchain.StartNew(Keccak.EmptyTreeHash);

        const byte b1Value = 1;
        block1.SetAccount(Key0, new Account(b1Value, b1Value));
        block1.SetStorage(Key0, Key1, stackalloc byte[1] { b1Value });

        var hash1 = block1.Commit(1);

        const byte b2Value = 2;
        using var block2 = blockchain.StartNew(hash1);

        // destroy previous
        block2.DestroyAccount(Key0);
        block2.SetAccount(Key0, new Account(b2Value, b2Value));
        block2.SetStorage(Key0, Key1, stackalloc byte[1] { b2Value });

        // some additional
        block2.SetAccount(Key1, new Account(3, 4));

        var hash2 = block2.Commit(2);

        using var block3 = blockchain.StartNew(hash2);

        block3.GetStorage(Key0, Key1, stackalloc byte[32])
            .ToArray().Should().BeEquivalentTo(new[] { b2Value });

        var actual = block3.GetAccount(Key0);
        actual.Balance.Should().Be(b2Value);
        actual.Nonce.Should().Be(b2Value);
    }

    [Test]
    public async Task Account_destruction_database_flushed()
    {
        uint blockNo = 1;

        using var db = PagedDb.NativeMemoryDb(1 * Mb, 2);
        await using var blockchain = new Blockchain(db, new ComputeMerkleBehavior());

        using var worldState = blockchain.StartNew(Keccak.EmptyTreeHash);

        worldState.SetAccount(Key1, new Account(2, 2));

        var before = worldState.Commit(blockNo++);
        await blockchain.Finalize(before);

        worldState.SetAccount(Key0, new Account(1, 1));
        worldState.SetStorage(Key0, Key1, [1]);

        var hash = worldState.Commit(blockNo++);

        await blockchain.Finalize(hash);

        worldState.DestroyAccount(Key0);
        var hash2 = worldState.Commit(blockNo);

        await blockchain.Finalize(hash2);

        using var read = db.BeginReadOnlyBatch();

        read.Metadata.BlockNumber.Should().Be(3);

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

        var behavior = new ComputeMerkleBehavior();

        await using (var blockchain = new Blockchain(db, behavior))
        {
            var hash = Keccak.EmptyTreeHash;

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
                await blockchain.Finalize(keccak2A);

                // start in the past
                using (var block2B = blockchain.StartNew(keccak1A))
                {
                    block2B.GetAccount(Key0).Should().Be(account1);
                }
            }
        }
    }

    [Test]
    public async Task Same_hash_same_block_number()
    {
        var value = new Account(1, 1);
        var start = Keccak.EmptyTreeHash;

        using var db = PagedDb.NativeMemoryDb(1 * Mb, 4);

        await using var blockchain = new Blockchain(db, new ComputeMerkleBehavior());

        var keccak1A = CommitOnce();
        var keccak1B = CommitOnce();

        keccak1A.Should().Be(keccak1B);

        blockchain.HasState(keccak1A).Should().BeTrue();

        Keccak CommitOnce()
        {
            using var block = blockchain.StartNew(start);
            block.SetAccount(Key0, value);
            return block.Commit(1);
        }
    }

    [Test]
    public async Task Respects_commit_cache_budget()
    {
        const int commitCacheLimit = 4;

        using var db = PagedDb.NativeMemoryDb(1 * Mb);

        var cacheBudgetPreCommit = new CacheBudget.Options(1, 1);

        await using var blockchain = new Blockchain(db, new ComputeMerkleBehavior(), null, CacheBudget.Options.None,
            cacheBudgetPreCommit, 1, int.MaxValue, null);

        // Initial commit
        using var start = blockchain.StartNew(Keccak.EmptyTreeHash);
        const uint startNumber = 1;
        start.SetAccount(Keccak.OfAnEmptyString, new Account(startNumber, startNumber));
        var root = start.Commit(startNumber);

        for (uint i = startNumber + 1; i < commitCacheLimit + 10; i++)
        {
            using var block = blockchain.StartNew(root);
            block.SetAccount(Keccak.OfAnEmptyString, new Account(i, i));
            root = block.Commit(i);

            block.Stats.DbReads.Should().BeLessThanOrEqualTo(1,
                "Because the only read that is required for the Merkle leaf, should be cached transiently");

            // finalize as soon as possible to destroy dependencies
            blockchain.Finalize(root);
        }
    }

    [Test]
    public async Task Read_accessor()
    {
        const byte historyDepth = 16;
        using var db = PagedDb.NativeMemoryDb(16 * Mb, historyDepth);

        await using var blockchain = new Blockchain(db, new ComputeMerkleBehavior());

        var accessor = blockchain.BuildReadOnlyAccessor();

        const int count = 128;

        var parent = Keccak.EmptyTreeHash;

        var hashes = new Keccak[count + 1];
        hashes[0] = parent;

        for (uint i = 0; i < count; i++)
        {
            using var block = blockchain.StartNew(parent);
            block.SetAccount(Key(i), Value(i));
            parent = hashes[i + 1] = block.Commit(i + 1);
        }

        var h = hashes[historyDepth];

        await blockchain.Finalize(h);

        // omit 0th
        for (uint i = 10; i < count; i++)
        {
            var root = hashes[i + 1];

            accessor.HasState(root).Should().BeTrue();
            accessor.GetAccount(root, Key(i)).Should().Be(Value(i));
        }

        return;

        static Keccak Key(uint i)
        {
            Keccak k = default;
            BinaryPrimitives.WriteUInt32LittleEndian(k.BytesAsSpan, i + 3);
            return k;
        }

        static Account Value(uint i) => new(i, i);
    }

    [Test]
    public async Task Read_accessor_can_preload_readers_for_history()
    {
        const byte historyDepth = 16;
        using var db = PagedDb.NativeMemoryDb(16 * Mb, historyDepth);

        const int count = 128;
        var hashes = new Keccak[count + 1];

        await using (var blockchain = new Blockchain(db, new ComputeMerkleBehavior()))
        {
            var parent = Keccak.EmptyTreeHash;
            hashes[0] = parent;

            for (uint i = 0; i < count; i++)
            {
                using var block = blockchain.StartNew(parent);
                block.SetAccount(Key(i), Value(i));
                parent = hashes[i + 1] = block.Commit(i + 1);
            }

            // Flush the last
            await blockchain.Finalize(parent);
        }

        // Reload blockchain so that accessor is built from zero
        await using var reloaded = new Blockchain(db, new ComputeMerkleBehavior());
        var accessor = reloaded.BuildReadOnlyAccessor();

        // Assert only last historyDepth hashes
        for (uint i = count - historyDepth + 1; i < count; i++)
        {
            var root = hashes[i + 1];

            accessor.HasState(root).Should()
                .BeTrue($"Failed to properly assert at {i} out of {count}.");
            accessor.GetAccount(root, Key(i)).Should().Be(Value(i));
        }

        return;

        static Keccak Key(uint i)
        {
            Keccak k = default;
            BinaryPrimitives.WriteUInt32LittleEndian(k.BytesAsSpan, i + 3);
            return k;
        }

        static Account Value(uint i) => new(i, i);
    }


    [Test]
    public async Task StartNew_when_throws_should_not_lock_db_readonly_batch()
    {
        using var db = PagedDb.NativeMemoryDb(1 * Mb, 2);

        await using var blockchain = new Blockchain(db, new PreCommit());

        var expected = db.CountReadOnlyBatches();

        var nonExistentParent = new Random(13).NextKeccak();
        try
        {
            var exception = Assert.Throws<Exception>(() => blockchain.StartNew(nonExistentParent));
            exception.Message.Should().Contain("There is no root page with the given stateHash");
        }
        finally
        {
            db.CountReadOnlyBatches().Should().Be(expected);
        }
    }

    private static Account GetAccount(int i) => new((UInt256)i + 1, (UInt256)i + 1);

    private static Keccak BuildKey(int i)
    {
        Span<byte> span = stackalloc byte[4];
        BinaryPrimitives.WriteInt32LittleEndian(span, i);
        return Keccak.Compute(span);
    }

    private class PreCommit : IPreCommitBehavior
    {
        public Keccak BeforeCommit(ICommitWithStats commit, CacheBudget budget, bool isSnapSync = false)
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