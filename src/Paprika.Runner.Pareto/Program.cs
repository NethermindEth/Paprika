using System.Diagnostics.CodeAnalysis;
using System.Diagnostics.Metrics;
using System.Runtime.InteropServices;
using Paprika.Chain;
using Paprika.Crypto;
using Paprika.Merkle;
using Paprika.Store;
using Paprika.Tests;
using Paprika.Utils;
using Spectre.Console;

[assembly: ExcludeFromCodeCoverage]

namespace Paprika.Runner.Pareto;

public static class Program
{
    private const int MaxReorgDepth = 64;
    private const int FinalizeEvery = 128;

    private const int BlockCount = 50_000;

    private const int AccountCount = 1_000_000;
    private const int ContractCount = 50_000;
    private const int MinAccountsPerBlock = 200;
    private const int MaxAccountsPerBlock = 500;

    private const int MaxStorageSlotsCount = 1_000_000;
    private const int MinStoragePerContract = 10;
    private const int MaxStoragePerContract = 500;

    private const int RandomSeed = 17;
    private const long DbFileSize = 32 * Gb;
    private const long Gb = 1024 * 1024 * 1024L;

    public static async Task Main(String[] args)
    {
        var random = new Random(RandomSeed);
        var keccaks = new Keccak[AccountCount];
        random.NextBytes(MemoryMarshal.Cast<Keccak, byte>(keccaks));

        Task reportingTask = Task.CompletedTask;

        var spectre = new CancellationTokenSource();

        using var meter = new Meter("Paprika.Runner");
        var atBlock = meter.CreateAtomicObservableGauge("At block", "block number");
        var gaugeAccountsPerBlock = meter.CreateAtomicObservableGauge("Accounts amended last block", "count");
        var gaugeContractsPerBlock = meter.CreateAtomicObservableGauge("Contracts amended last block", "count");
        var gaugeStorageSlotPerBlock = meter.CreateAtomicObservableGauge("Storage slots set last block", "count");

        try
        {
            using var reporter = new MetricsReporter();

            var dir = Directory.GetCurrentDirectory();
            var dataPath = Path.Combine(dir, "db");

            if (Directory.Exists(dataPath))
            {
                Console.WriteLine("Deleting previous db...");
                Directory.Delete(dataPath, true);
            }

            Directory.CreateDirectory(dataPath);
            Console.WriteLine($"Using persistent DB on disk, located: {dataPath}");

            Console.WriteLine("Initializing db of size {0}GB", DbFileSize / Gb);

            using var db = PagedDb.MemoryMappedDb(DbFileSize, MaxReorgDepth, dataPath);

            // ReSharper disable once MethodSupportsCancellation
#pragma warning disable CS4014

            reportingTask = Task.Run(() => AnsiConsole.Live(reporter.Renderer)
#pragma warning restore CS4014
                .StartAsync(async ctx =>
                {
                    while (spectre.IsCancellationRequested == false)
                    {
                        reporter.Observe();
                        ctx.Refresh();
                        await Task.Delay(500);
                    }

                    // the final report
                    reporter.Observe();
                    ctx.Refresh();
                }));

            using var preCommit =
                new ComputeMerkleBehavior(ComputeMerkleBehavior.ParallelismNone);

            var blockHash = Keccak.EmptyTreeHash;
            var finalization = new Queue<Keccak>();

            // add finality and 10 just to make it a bit slower
            var gate = new SingleAsyncGate(FinalizeEvery + 10);

            var cacheBudgetStateAndStorage = new CacheBudget.Options(2_000, 16);
            var cacheBudgetPreCommit = new CacheBudget.Options(2_000, 16);

            await using (var blockchain = new Blockchain(db, preCommit, TimeSpan.FromSeconds(5),
                             cacheBudgetStateAndStorage, cacheBudgetPreCommit, 1000, reporter.Observe))
            {
                blockchain.Flushed += (_, e) => gate.Signal(e.blockNumber);

                uint at = 1;

                // 500_000 * 50 = 25_000_000 z 10_000_000
                for (; at < BlockCount; at++)
                {
                    var accountSetCount = 0;
                    var contractSetCount = 0;
                    var storageSlotCount = 0;

                    atBlock.Set((int)at);

                    using var block = blockchain.StartNew(blockHash);

                    var accounts = random.Next(MinAccountsPerBlock, MaxAccountsPerBlock);

                    for (var j = 0; j < accounts; j++)
                    {
                        var index = random.Next(AccountCount);
                        var isContract = index < ContractCount;

                        if (isContract)
                        {
                            contractSetCount++;

                            // regenerate index, so that it's skewed and more probable to be close to zero
                            index = ContractCount - RandomPareto(random, ContractCount);

                            var accountKeccak = keccaks[index];
                            var account = block.GetAccount(accountKeccak);

                            // increment the account
                            block.SetAccount(accountKeccak, new Account(account.Balance + 1, account.Nonce + 1));

                            var maxSlotsForThisAccount = GetMaxSlotsForContractAt(index);
                            var count = random.Next(MinStoragePerContract, MaxStoragePerContract);

                            for (var k = 0; k < count; k++)
                            {
                                storageSlotCount++;

                                var slot = random.Next(maxSlotsForThisAccount);
                                var storageKeccak = keccaks[slot];
                                block.SetStorage(accountKeccak, storageKeccak, random.NextKeccak().BytesAsSpan);
                            }
                        }
                        else
                        {
                            accountSetCount++;

                            // increment the account
                            var accountKeccak = keccaks[index];
                            var account = block.GetAccount(accountKeccak);
                            block.SetAccount(accountKeccak, new Account(account.Balance + 11, account.Nonce + 13));
                        }
                    }

                    gaugeAccountsPerBlock.Set(accountSetCount);
                    gaugeStorageSlotPerBlock.Set(storageSlotCount);
                    gaugeContractsPerBlock.Set(contractSetCount);

                    blockHash = block.Commit(at);

                    await gate.WaitAsync(at);

                    finalization.Enqueue(blockHash);

                    if (finalization.Count == FinalizeEvery)
                    {
                        blockchain.Finalize(finalization.Dequeue());
                    }
                }

                // finalize the last one
                blockchain.Finalize(finalization.Last());
            }

            using var read = db.BeginReadOnlyBatch();

            var state = new StatisticsReporter();
            var storage = new StatisticsReporter();
            read.Report(state, storage);

            spectre.Cancel();
            await reportingTask;
        }
        catch (Exception e)
        {
            spectre.Cancel();
            await reportingTask;

            var paragraph = new Paragraph();
            var style = new Style().Foreground(Color.Red);
            paragraph.Append(e.Message, style);
            paragraph.Append(e.StackTrace!, style);

            AnsiConsole.Write(paragraph);
        }
    }

    private static int GetMaxSlotsForContractAt(int index)
    {
        // Make contracts storage skewed.
        // Contracts with low indexes should have a lot of entries, with higher, much less
        return MaxStorageSlotsCount / (1 + index);
    }

    private static int RandomPareto(Random random, int maxValue)
    {
        // The result is skewed towards max

        const double alpha = 1.5; // Shape parameter

        var u = random.NextDouble(); // Uniformly distributed number between 0 and 1
        return
            (int)(maxValue *
                  Math.Pow(u, 1.0 / alpha)); // Inverse of the CDF (Cumulative Distribution Function) for Pareto
    }
}
