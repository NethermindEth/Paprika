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
            db.ForbidReads((in Key key) => key.Type == DataType.Merkle);

            block.SetAccount(keccak, new Account(i, i));
            parent = block.Commit(i);

            blockchain.Finalize(parent);
            await wait.WaitAsync();
        }
    }

    private static void Set(Keccak[] accounts, uint account, IWorldState start, UInt256 bigNonce)
    {
        ref var k = ref accounts[account];
        BinaryPrimitives.WriteUInt32LittleEndian(k.BytesAsSpan, account);
        start.SetAccount(k, new Account(13, bigNonce));
    }

    [Explicit]
    [TestCase(true, Category = Categories.LongRunning)]
    [TestCase(false, Category = Categories.LongRunning)]
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