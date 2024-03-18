using NUnit.Framework;
using Paprika.Chain;
using Paprika.Crypto;
using Paprika.Merkle;
using Paprika.Store;

namespace Paprika.Tests.Merkle;

public class ReadBehaviorTests
{
    [Test]
    [Category(Categories.LongRunning)]
    public async Task Test()
    {
        using var db = PagedDb.NativeMemoryDb(128 * 1024 * 1024, 2);
        var cache = new CacheBudget.Options(5000, 8);
        await using var blockchain = new Blockchain(db, new ComputeMerkleBehavior(), null, cache, cache);

        const int blocks = 1000;
        const int accountsPerBlock = 100;
        const int storagePerAccount = 100;
        const int seed = 13;

        var collector = new MetricsCollector();
        var random = new Random(seed);

        var value = new byte[] { 17 };

        var parent = Keccak.Zero;

        for (uint i = 0; i < blocks; i++)
        {
            collector.Clear();

            using var block = blockchain.StartNew(parent, collector);

            for (var account = 0; account < accountsPerBlock; account++)
            {
                var addr = random.NextKeccak();
                block.SetAccount(addr, BuildAccount(i));

                for (var storage = 0; storage < storagePerAccount; storage++)
                {
                    block.SetStorage(addr, random.NextKeccak(), value);
                }
            }

            parent = block.Commit(i + 1);
        }

        Console.WriteLine(collector.Report(2));
        return;

        static Account BuildAccount(uint block) => new(block + 1, block + 1);
    }
}