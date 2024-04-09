using System.Diagnostics;
using System.Runtime.InteropServices;
using FluentAssertions;
using Nethermind.Int256;
using Paprika.Chain;
using Paprika.Crypto;
using Paprika.Merkle;
using Paprika.Store;

namespace Paprika.Tests.Chain;


public class PrefetchingTests
{
    [Test]
    public async Task Prefetches_properly_on_not_changed_structure()
    {
        using var original = PagedDb.NativeMemoryDb(32 * 1024 * 1024, 2);
        var db = new ReadForbiddingDb(original);

        var merkle = new ComputeMerkleBehavior(ComputeMerkleBehavior.ParallelismNone);
        await using var blockchain = new Blockchain(db, merkle);

        const int blocks = 100;
        var bigNonce = new UInt256(100, 100, 100, 100);

        var random = new Random(13);
        var accounts = new Keccak[blocks];
        random.NextBytes(MemoryMarshal.Cast<Keccak, byte>(accounts));
        
        // Create structure first
        var parent = Keccak.EmptyTreeHash;

        using var start = blockchain.StartNew(parent);

        for (uint account = 0; account < blocks; account++)
        {
            
            start.SetAccount(accounts[account], new Account(13, bigNonce));
        }

        parent = start.Commit(1);
        blockchain.Finalize(parent);
        await blockchain.WaitTillFlush(1);

        for (uint i = 2; i < blocks; i++)
        {
            var keccak = accounts[i];
            using var block = blockchain.StartNew(parent);

            db.MerkleReadsForbid(false);
            
            // prefetch first
            var p = block.OpenPrefetcher();
            p!.CanPrefetchFurther.Should().BeTrue();
            p.PrefetchAccount(keccak);
            
            // forbid reads
            db.MerkleReadsForbid(true);

            block.SetAccount(keccak, new Account(i, bigNonce));
            parent = block.Commit(i);
            
            blockchain.Finalize(parent);
            await blockchain.WaitTillFlush(i);
        }
    }
    
    [Explicit]
    [TestCase(true)]
    [TestCase(false)]
    public async Task Spin(bool prefetch)
    {
        var commits = new Stopwatch();

        const int parallelism = ComputeMerkleBehavior.ParallelismNone;
        const int finalityLength = 16;
        const int accounts = 1_000_000;
        const int accountsPerBlock = 50;
        const int blocks = accounts / accountsPerBlock;

        var random = new Random(13);
        var keccaks = new Keccak[accounts];

        random.NextBytes(MemoryMarshal.Cast<Keccak, byte>(keccaks));

        using var db = PagedDb.NativeMemoryDb(1024 * 1024 * 1024, 2);
        var merkle = new ComputeMerkleBehavior(parallelism);
        await using var blockchain = new Blockchain(db, merkle);
        var at = 0;
        var parent = Keccak.EmptyTreeHash;
        var finality = new Queue<Keccak>();
        var prefetchFailures = 0;

        for (uint i = 1; i < blocks; i++)
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
                    }

                    return true;
                });

            await Task.WhenAll(Task.Delay(50), task);

            if ((await task) == false)
                prefetchFailures++;

            SetAccounts(slice, block, i);

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

    private static void SetAccounts(ReadOnlyMemory<Keccak> slice, IWorldState block, uint i)
    {
        foreach (var keccak in slice.Span)
        {
            block.SetAccount(keccak, new Account(i, i));
        }
    }
}