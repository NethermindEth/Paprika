using System.Buffers.Binary;
using System.Diagnostics;
using System.Runtime.InteropServices;
using Paprika.Chain;
using Paprika.Crypto;
using Paprika.Merkle;
using Paprika.Store;

namespace Paprika.Tests.Chain;

[ReportTime]
public class PrefetchingTests
{
    [TestCase(true)]
    [TestCase(false)]
    public async Task Test(bool prefetch)
    {
        const int parallelism = ComputeMerkleBehavior.ParallelismNone;
        
        const int accounts = 10_000;
        const int blocks = 1000;
        const int accountsPerBlock = 10;

        var random = new Random(13);
        var keccaks = new Keccak[accounts];

        random.NextBytes(MemoryMarshal.Cast<Keccak, byte>(keccaks));
        
        using var db = PagedDb.NativeMemoryDb(128 * 1024 * 1024, 2);
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
            
            await Task.WhenAll(Task.Delay(20), task);

            if ((await task) == false)
                prefetchFailures++;
            
            SetAccounts(slice, block, i);
            
            parent = block.Commit(i);
            
            finality.Enqueue(parent);
            if (finality.Count > 32)
            {
                blockchain.Finalize(finality.Dequeue());
            }
        }

        while (finality.TryDequeue(out var k))
        {
            blockchain.Finalize(k);
        }
        
        Console.WriteLine($"Prefetch failures: {prefetchFailures}");
    }

    private static void SetAccounts(ReadOnlyMemory<Keccak> slice, IWorldState block, uint i)
    {
        foreach (var keccak in slice.Span)
        {
            block.SetAccount(keccak, new Account(i,i));
        }
    }
}