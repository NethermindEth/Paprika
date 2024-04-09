using System.Diagnostics;
using System.Runtime.InteropServices;
using Paprika.Chain;
using Paprika.Crypto;
using Paprika.Merkle;
using Paprika.Store;

namespace Paprika.Tests.Chain;

[Explicit]
public class PrefetchingTests
{
    [TestCase(true)]
    [TestCase(false)]
    public async Task Test(bool prefetch)
    {
        var commits = new Stopwatch();

        const int parallelism = ComputeMerkleBehavior.ParallelismNone;
        const int finalityLength = 16;
        const int accounts = 500_000;
        const int accountsPerBlock = 50;
        const int blocks = accounts / accountsPerBlock;

        var random = new Random(13);
        var keccaks = new Keccak[accounts];

        random.NextBytes(MemoryMarshal.Cast<Keccak, byte>(keccaks));

        using var db = PagedDb.NativeMemoryDb(512 * 1024 * 1024, 2);
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