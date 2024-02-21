using FluentAssertions;
using NUnit.Framework;
using Paprika.Chain;
using Paprika.Crypto;
using Paprika.Merkle;
using Paprika.Store;
using Paprika.Utils;

namespace Paprika.Tests.Utils;

public class MetricsTests
{
    private const int Mb = 1024 * 1024;

    [Test]
    [Explicit("Sometimes metrics do not report as it's HDR reporting.")]
    public async Task Metrics_should_report()
    {
        using var metrics = new Metrics();

        using var db = PagedDb.NativeMemoryDb(16 * Mb, 2);

        await using var blockchain = new Blockchain(db, new ComputeMerkleBehavior(1, 1, Memoization.None));

        var random = new Random(13);
        var parent = Keccak.EmptyTreeHash;
        var finality = new Queue<Keccak>();
        var value = new byte[13];

        const uint spins = 100;
        const int accountsPerSpin = 100;

        for (uint at = 1; at < spins; at++)
        {
            using var block = blockchain.StartNew(parent);

            Keccak k = default;
            for (int i = 0; i < accountsPerSpin; i++)
            {
                random.NextBytes(k.BytesAsSpan);
                block.SetAccount(k, new Account(at, at));
            }

            // set storage for the last
            block.SetStorage(k, k, value);

            parent = block.Commit(at + 1);
            finality.Enqueue(parent);

            if (finality.Count > 64)
            {
                blockchain.Finalize(finality.Dequeue());
            }

            metrics.Observe();
        }

        while (finality.TryDequeue(out var finalized))
        {
            blockchain.Finalize(finalized);
        }

        metrics.Merkle.TotalMerkle.Value.Should().BeGreaterThan(0);
        metrics.Merkle.StateProcessing.Value.Should().BeGreaterThan(0);
        metrics.Merkle.StorageProcessing.Value.Should().BeGreaterThan(0);
        metrics.Db.DbSize.Value.Should().BeGreaterThan(0);
    }
}