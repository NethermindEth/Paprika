using System.Runtime.CompilerServices;
using Nethermind.Int256;
using NUnit.Framework;
using Paprika.Chain;
using Paprika.Crypto;
using Paprika.Merkle;
using Paprika.Store;

namespace Paprika.Tests;

[Explicit("Performance tests are only to be run on an imported database")]
public class Performance
{
    private const long GB = 1024 * 1024 * 1024;

    [Test]
    public async Task Read_non_existing()
    {
        const byte historyDepth = 64;
        const long size = 64 * GB;

        var random = new Random(17);
        var value = new byte[] { 13 };

        var path = ImportedDbPath();

        using var db = PagedDb.MemoryMappedDb(size, historyDepth, path);
        var cache = new CacheBudget.Options(5000, 16);
        var merkle = new ComputeMerkleBehavior();

        await using var blockchain = new Blockchain(db, merkle, TimeSpan.FromSeconds(10), cache, cache);

        // Create 64 blocks, then try to read non-existing accounts

        using var latest = db.BeginReadOnlyBatchOrLatest(default);

        var parent = latest.Metadata.StateHash;
        var parentNumber = latest.Metadata.BlockNumber;

        for (var atBlock = 0; atBlock < 64; atBlock++)
        {
            using var block = blockchain.StartNew(parent);

            for (var atAccount = 0; atAccount < 256; atAccount++)
            {
                Keccak account = default;
                random.NextBytes(account.BytesAsSpan);

                block.SetAccount(account, new Account(121, 23423));

                for (var atStorage = 0; atStorage < 10; atStorage++)
                {
                    Keccak storage = default;
                    random.NextBytes(storage.BytesAsSpan);

                    block.SetStorage(account, storage, value);
                }
            }

            parent = block.Commit(parentNumber + 1);
            parentNumber++;
        }

        using var read = blockchain.StartNew(parent);
        DoNonExistentReads(read, random);

        static void DoNonExistentReads(IWorldState block, Random random)
        {
            UInt256 sum = 0;

            for (var atAccount = 0; atAccount < 10_000; atAccount++)
            {
                Keccak account = default;
                random.NextBytes(account.BytesAsSpan);

                sum += block.GetAccount(account).Balance;
            }
        }
    }

    private static string ImportedDbPath([CallerFilePath] string path = "")
    {
        var dir = Path.GetDirectoryName(path);
        return Path.Combine(dir, "..\\Paprika.Importer\\db");
    }
}
