using System.Buffers.Binary;
using System.Diagnostics;
using System.Runtime.InteropServices;
using FluentAssertions;
using Nethermind.Int256;
using Paprika.Chain;
using Paprika.Crypto;
using Paprika.Data;
using Paprika.Merkle;
using Paprika.Store;

namespace Paprika.Tests.Chain;

public class PrefetchingTests
{
    [Test]
    public async Task Prefetches_properly_on_not_changed_structure()
    {
        using var original = PagedDb.NativeMemoryDb(8 * 1024 * 1024, 2);
        var db = new ReadForbiddingDb(original);

        var merkle = new ComputeMerkleBehavior(ComputeMerkleBehavior.ParallelismNone);
        await using var blockchain = new Blockchain(db, merkle);

        var wait = new SemaphoreSlim(0, 1000);
        blockchain.Flushed += (_, _) => wait.Release();

        // Nicely fill 2 levels of the tree, so that RlpMemo.Compress does not remove branches with two children
        const int blocks = 256;
        var bigNonce = new UInt256(100, 100, 100, 100);

        var accounts = new Keccak[blocks];

        // Create structure first
        var parent = Keccak.EmptyTreeHash;

        using var start = blockchain.StartNew(parent);

        for (uint account = 0; account < blocks; account++)
        {
            Set(accounts, account, start, bigNonce);
        }

        parent = start.Commit(1);
        blockchain.Finalize(parent);

        await wait.WaitAsync();

        for (uint i = 2; i < blocks; i++)
        {
            var keccak = accounts[i];
            using var block = blockchain.StartNew(parent);

            db.AllowAllReads();

            // prefetch first
            var p = block.OpenPrefetcher();
            p!.CanPrefetchFurther.Should().BeTrue();
            p.PrefetchAccount(keccak);

            // forbid reads
            db.ForbidReads((in Key key) => key.Type == DataType.Merkle &&
                                           key.Path.Length > ComputeMerkleBehavior.SkipRlpMemoizationForTopLevelsCount);

            block.SetAccount(keccak, new Account(i, i));
            parent = block.Commit(i);

            blockchain.Finalize(parent);
            await wait.WaitAsync();
        }
    }

    [Test]
    public async Task Makes_all_decompression_on_prefetch()
    {
        using var db = PagedDb.NativeMemoryDb(8 * 1024 * 1024, 2);
        var merkle = new ComputeMerkleBehavior(ComputeMerkleBehavior.ParallelismNone);
        await using var blockchain = new Blockchain(db, merkle);

        // Create one block with some values, commit it and finalize
        var hash = Keccak.EmptyTreeHash;

        hash = BuildBlock(blockchain, hash, 1);
        blockchain.Finalize(hash);
        await blockchain.WaitTillFlush(hash);

        hash = BuildBlock(blockchain, hash, 2);

        return;

        static Keccak BuildBlock(Blockchain blockchain, Keccak parent, uint number)
        {
            var isFirst = number == 1;

            byte[] value = isFirst ? [17] : [23];

            const int seed = 13;
            const int contracts = 10;
            const int slots = 10;

            using var block = blockchain.StartNew(parent);
            var random = new Random(seed);

            // Open prefetcher on blocks beyond first
            IPreCommitPrefetcher? prefetcher = null;

            for (var i = 0; i < contracts; i++)
            {
                var contract = random.NextKeccak();
                prefetcher?.PrefetchAccount(contract);

                if (isFirst)
                {
                    block.SetAccount(contract, new Account(1, 1, Keccak.Zero, Keccak.Zero));
                }

                for (var j = 0; j < slots; j++)
                {
                    var storage = random.NextKeccak();
                    prefetcher?.PrefetchStorage(contract, storage);
                    block.SetStorage(contract, storage, value);
                }
            }

            prefetcher?.SpinTillPrefetchDone();

            using (RlpMemo.NoDecompression())
            {
                return block.Commit(number);
            }
        }
    }

    private static void Set(Keccak[] accounts, uint account, IWorldState start, UInt256 bigNonce)
    {
        ref var k = ref accounts[account];
        BinaryPrimitives.WriteUInt32LittleEndian(k.BytesAsSpan, account);
        start.SetAccount(k, new Account(13, bigNonce));
    }

    [Explicit]
    [TestCase(true, false, Category = Categories.LongRunning, TestName = "No storage, prefetch")]
    [TestCase(false, false, Category = Categories.LongRunning, TestName = "No storage, no prefetch")]
    [TestCase(true, true, Category = Categories.LongRunning, TestName = "Storage, prefetch")]
    [TestCase(false, true, Category = Categories.LongRunning, TestName = "Storage, no prefetch")]
    public async Task Spin(bool prefetch, bool storage)
    {
        const int parallelism = ComputeMerkleBehavior.ParallelismNone;
        const int finalityLength = 16;
        const int accounts = 50_000;
        const int accountsPerBlock = 100;
        const int blocks = accounts / accountsPerBlock;

        var random = new Random(13);
        var keccaks = new Keccak[accounts];

        random.NextBytes(MemoryMarshal.Cast<Keccak, byte>(keccaks));

        using var db = PagedDb.NativeMemoryDb(1024 * 1024 * 1024, 2);
        var merkle = new ComputeMerkleBehavior(parallelism);
        await using var blockchain = new Blockchain(db, merkle);

        const uint startBlockNumber = 1;
        var parent = Keccak.EmptyTreeHash;

        // Setup test by creating all the account first.
        // This should ensure that he Merkle construct is created and future updates should be prefetched properly without additional db reads 
        using var first = blockchain.StartNew(parent);
        SetAccounts(new ReadOnlyMemory<Keccak>(keccaks), first, startBlockNumber, storage);
        parent = first.Commit(startBlockNumber);
        blockchain.Finalize(parent);
        await blockchain.WaitTillFlush(startBlockNumber);

        // Run commits now with a prefetching
        await RunBlocksWithPrefetching(blockchain, keccaks, parent, prefetch, storage);
        return;

        static async Task RunBlocksWithPrefetching(Blockchain blockchain, Keccak[] keccaks, Keccak parent,
            bool prefetch, bool storage)
        {
            var finality = new Queue<Keccak>();
            var prefetchFailures = 0;
            var at = 0;

            var commits = new Stopwatch();

            for (var i = startBlockNumber + 1; i < blocks + startBlockNumber + 1; i++)
            {
                using var block = blockchain.StartNew(parent);

                var slice = keccaks.AsMemory(at % accounts, accountsPerBlock);
                at += accountsPerBlock;

                // Execution delay
                var task = !prefetch
                    ? Task.FromResult(true)
                    : Task.Factory.StartNew(() =>
                    {
                        var prefetcher = block.OpenPrefetcher();
                        if (prefetcher == null)
                            return true;

                        foreach (var keccak in slice.Span)
                        {
                            if (prefetcher.CanPrefetchFurther == false)
                            {
                                return false;
                            }

                            prefetcher.PrefetchAccount(keccak);
                            if (storage)
                            {
                                prefetcher.PrefetchStorage(keccak, keccak);
                            }
                        }

                        return true;
                    });

                await Task.WhenAll(Task.Delay(50), task);

                if ((await task) == false)
                    prefetchFailures++;

                SetAccounts(slice, block, i, storage);

                commits.Start();
                parent = block.Commit(i);
                commits.Stop();

                finality.Enqueue(parent);

                if (finality.Count > finalityLength)
                {
                    blockchain.Finalize(finality.Dequeue());
                }
            }

            while (finality.TryDequeue(out var k))
            {
                blockchain.Finalize(k);
            }

            Console.WriteLine($"Prefetch failures: {prefetchFailures}. Commit time {commits.Elapsed:g}");
        }

        static void SetAccounts(ReadOnlyMemory<Keccak> slice, IWorldState block, uint i, bool storage)
        {
            Span<byte> value = stackalloc byte[sizeof(uint)];
            BinaryPrimitives.WriteUInt32LittleEndian(value, i);

            foreach (var keccak in slice.Span)
            {
                block.SetAccount(keccak, new Account(i, i));
                if (storage)
                {
                    block.SetStorage(keccak, keccak, value);

                    if (i == startBlockNumber)
                    {
                        // Additional slot for the start, so that a branch is created and there's something to prefetch.
                        block.SetStorage(keccak, Keccak.EmptyTreeHash, value);
                    }
                }
            }
        }
    }
}