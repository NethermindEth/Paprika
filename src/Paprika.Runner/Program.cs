using System.Buffers.Binary;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Reflection;
using System.Reflection.Metadata;
using System.Text;
using Nethermind.Int256;
using Paprika.Chain;
using Paprika.Crypto;
using Paprika.Data;
using Paprika.Merkle;
using Paprika.Store;
using Paprika.Tests;
using Spectre.Console;

[assembly: ExcludeFromCodeCoverage]

namespace Paprika.Runner;

/// <summary>
/// The case for running the runner.
/// </summary>
public record Case(
    uint BlockCount,
    int AccountsPerBlock,
    long DbFileSize,
    bool PersistentDb,
    TimeSpan FlushEvery,
    bool Fsync,
    bool UseBigStorageAccount)
{
    public static uint NumberOfLogs => 20u;
    public uint LogEvery => BlockCount / NumberOfLogs;
}

public static class Program
{
    private static readonly Case InMemoryReallySmall =
        new(100, 1000, 1 * Gb, false, TimeSpan.FromSeconds(5), false, true);

    private static readonly Case InMemorySmall =
        new(10_000, 1000, 10 * Gb, false, TimeSpan.FromSeconds(5), false, true);

    private static readonly Case InMemoryMedium =
        new(50_000, 1000, 32 * Gb, false, TimeSpan.FromSeconds(5), false, false);

    private static readonly Case
        InMemoryBig = new(100_000, 1000, 56 * Gb, false, TimeSpan.FromSeconds(5), false, false);

    private static readonly Case DiskSmallNoFlush =
        new(50_000, 1000, 11 * Gb, true, TimeSpan.FromSeconds(5), false, false);

    private static readonly Case DiskSmallFlushFile =
        new(50_000, 1000, 32 * Gb, true, TimeSpan.FromSeconds(60), true, false);

    private const int MaxReorgDepth = 64;
    private const int FinalizeEvery = 64;

    private const int RandomSeed = 17;
    private const long Gb = 1024 * 1024 * 1024L;
    private const int UseStorageEveryNAccounts = 2;

    public static async Task Main(String[] args)
    {
        // select the case
        var caseName = (args.Length > 0 ? args[0] : nameof(InMemorySmall)).ToLowerInvariant();
        var cases = typeof(Program)
            .GetFields(BindingFlags.Static | BindingFlags.Public | BindingFlags.NonPublic)
            .Where(f => f.FieldType == typeof(Case))
            .ToDictionary(p => p.Name.ToLowerInvariant(), p => (Case)p.GetValue(null)!);

        var config = cases[caseName];

        const string left = "Left";
        const string metrics = "Metrics";
        const string info = "Info";
        const string right = "Right";
        const string writing = "Writing";
        const string reading = "Reading";

        var layout = new Layout("Runner")
            .SplitColumns(
                new Layout(left).SplitRows(new Layout(metrics), new Layout(info)),
                new Layout(right).SplitRows(new Layout(writing), new Layout(reading)));

        Task reportingTask = Task.CompletedTask;

        var spectre = new CancellationTokenSource();

        try
        {
            using var reporter = new MetricsReporter();

            layout[metrics].Update(reporter.Renderer);

            var dir = Directory.GetCurrentDirectory();
            var dataPath = Path.Combine(dir, "db");

            if (config.PersistentDb)
            {
                if (Directory.Exists(dataPath))
                {
                    Console.WriteLine("Deleting previous db...");
                    Directory.Delete(dataPath, true);
                }

                Directory.CreateDirectory(dataPath);
                Console.WriteLine($"Using persistent DB on disk, located: {dataPath}");
            }
            else
            {
                Console.WriteLine("Using in-memory DB for greater speed.");
            }

            Console.WriteLine("Initializing db of size {0}GB", config.DbFileSize / Gb);

            PagedDb db = config.PersistentDb
                ? PagedDb.MemoryMappedDb(config.DbFileSize, MaxReorgDepth, dataPath, config.Fsync)
                : PagedDb.NativeMemoryDb(config.DbFileSize, MaxReorgDepth);

            var random = BuildRandom();
            var bigStorageAccount = GetBigAccountKey();

            Console.WriteLine();
            Console.WriteLine("Writing:");
            Console.WriteLine("- {0} accounts per block through {1} blocks", config.AccountsPerBlock,
                config.BlockCount);
            Console.WriteLine("- it gives {0} total accounts", config.AccountsPerBlock * config.BlockCount);

            if (UseStorageEveryNAccounts > 0)
            {
                Console.WriteLine($"- every {UseStorageEveryNAccounts}th account will have 1 storage slot written");
            }

            if (config.UseBigStorageAccount)
            {
                Console.WriteLine("- each account amends 1 slot in Big Storage account");
            }

            int counter;

            // ReSharper disable once MethodSupportsCancellation
#pragma warning disable CS4014

            reportingTask = Task.Run(() => AnsiConsole.Live(layout)
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

            using var preCommit = new ComputeMerkleBehavior(ComputeMerkleBehavior.ParallelismNone);

            var gate = new SingleAsyncGate(FinalizeEvery + 10);
            //IPreCommitBehavior preCommit = null;

            await using (var blockchain =
                         new Blockchain(db, preCommit, config.FlushEvery, default, default, 1000, reporter.Observe))
            {
                blockchain.Flushed += (_, e) => gate.Signal(e.blockNumber);

                counter = Writer(config, blockchain, bigStorageAccount, random, layout[writing], gate);
            }

            // waiting for finalization
            using var read = db.BeginReadOnlyBatch("Runner - reading");

            var readingStopWatch = Stopwatch.StartNew();
            random = BuildRandom();

            var logReadEvery = counter / Case.NumberOfLogs;
            for (var i = 0; i < counter; i++)
            {
                var key = random.NextKeccak();
                var actual = read.GetAccount(key);

                var expected = GetAccountValue(i);

                var hasStorage = UseStorageEveryNAccounts > 0 && i % UseStorageEveryNAccounts == 0;

                if (hasStorage)
                {
                    // the account has the storage, compare only: balance, nonce and codehash as the storage will be different
                    if (!actual.Nonce.Equals(expected.Nonce) ||
                        !actual.Balance.Equals(expected.Balance) ||
                        !actual.CodeHash.Equals(expected.CodeHash) ||
                        actual.StorageRootHash.Equals(Keccak.EmptyTreeHash)) // should not be empty!
                    {
                        throw new InvalidOperationException(
                            $"Invalid account state for account number {i} with address {key.ToString()}. " +
                            $"The expected value is {expected} while the actual is {actual}!");
                    }

                    // compare storage
                    var storageAddress = GetStorageAddress(i);
                    var expectedStorageValue = GetStorageValue(i);
                    read.AssertStorageValue(key, storageAddress, expectedStorageValue);
                }
                else
                {
                    // no storage, compare fully
                    if (actual != expected)
                    {
                        throw new InvalidOperationException(
                            $"Invalid account state for account number {i} with address {key.ToString()}. " +
                            $"The expected value is {expected} while the actual is {actual}!");
                    }
                }

                if (config.UseBigStorageAccount)
                {
                    var storageAddress = GetStorageAddress(i);
                    var expectedStorageValue = GetBigAccountValue(i);
                    read.AssertStorageValue(bigStorageAccount, storageAddress, expectedStorageValue);
                }

                if (i > 0 & i % logReadEvery == 0)
                {
                    ReportReading(i);
                }
            }

            // the final report
            ReportReading(counter);

            // statistics
            StatisticsForPagedDb.Report(layout[info], read);

            spectre.Cancel();
            await reportingTask;

            void ReportReading(int i)
            {
                var secondsPerRead = TimeSpan.FromTicks(readingStopWatch.ElapsedTicks / logReadEvery).TotalSeconds;
                var readsPerSeconds = 1 / secondsPerRead;

                var txt = $"Reading at {i,9} out of {counter} accounts.\nCurrent speed: {readsPerSeconds:F1} reads/s";

                layout[reading].Update(new Panel(txt).Header(reading).Expand());

                readingStopWatch.Restart();
            }
        }
        catch (Exception e)
        {
            var paragraph = new Paragraph();

            var style = new Style().Foreground(Color.Red);
            paragraph.Append(e.Message, style);
            paragraph.Append(e.StackTrace!, style);

            layout[info].Update(new Panel(paragraph).Header(info).Expand());
            spectre.Cancel();

            await reportingTask;
            return;
        }

        layout[info].Update(new Panel(new Markup("[green]All data read and asserted correctly [/]")).Header(info)
            .Expand());
    }

    private static Random BuildRandom() => new(RandomSeed);

    private static int Writer(Case config, Blockchain blockchain, Keccak bigStorageAccount, Random random,
        Layout reporting, SingleAsyncGate gate)
    {
        var report = new StringBuilder();
        string result = "";
        var counter = 0;

        bool bigStorageAccountCreated = false;

        // writing
        var writing = Stopwatch.StartNew();
        var hash = Keccak.Zero;

        var toFinalize = new Queue<Keccak>();

        for (uint block = 1; block < config.BlockCount; block++)
        {
            using var worldState = blockchain.StartNew(hash);

            for (var account = 0; account < config.AccountsPerBlock; account++)
            {
                var key = random.NextKeccak();

                worldState.SetAccount(key, GetAccountValue(counter));

                if (UseStorageEveryNAccounts > 0 && counter % UseStorageEveryNAccounts == 0)
                {
                    var storageAddress = GetStorageAddress(counter);
                    var storageValue = GetStorageValue(counter);
                    worldState.SetStorage(key, storageAddress, storageValue);
                }

                if (config.UseBigStorageAccount)
                {
                    if (bigStorageAccountCreated == false)
                    {
                        worldState.SetAccount(bigStorageAccount, new Account(100, 100));
                        bigStorageAccountCreated = true;
                    }

                    var storageAddress = GetStorageAddress(counter);
                    var storageValue = GetBigAccountValue(counter);

                    worldState.SetStorage(bigStorageAccount, storageAddress, storageValue);
                }

                counter++;
            }

            hash = worldState.Commit(block);

            gate.WaitAsync(block).Wait();

            result = $"{hash.ToString()?[..8]}...";

            //finalize
            while (toFinalize.Count >= FinalizeEvery)
            {
                // finalize first
                blockchain.Finalize(toFinalize.Dequeue());
            }

            toFinalize.Enqueue(hash);

            if (block > 0 & block % config.LogEvery == 0)
            {
                report.AppendLine(
                    $@"At block {block,4}. This batch of {config.LogEvery} blocks took {writing.Elapsed:h\:mm\:ss\.FF}. RootHash: {result}");

                reporting.Update(new Panel(report.ToString()).Header("Writing").Expand());
                writing.Restart();
            }
        }

        // flush leftovers by adding one more block for now
        var lastBlock = toFinalize.Last();
        blockchain.Finalize(lastBlock);

        report.AppendLine(
            $@"At block {config.BlockCount - 1}. This batch of {config.LogEvery} blocks took {writing.Elapsed:h\:mm\:ss\.FF}. RootHash: {result}");

        reporting.Update(new Panel(report.ToString()).Header("Writing").Expand());

        return counter;
    }

    private static Account GetAccountValue(int counter)
    {
        return new Account((UInt256)counter, (UInt256)counter);
    }

    private static byte[] GetStorageValue(int counter) => ((UInt256)counter + 100000).ToBigEndian();

    private static Keccak GetBigAccountKey()
    {
        const string half = "0102030405060708090A0B0C0D0E0F";
        return NibblePath.Parse(half + half).UnsafeAsKeccak;
    }

    private static Keccak GetStorageAddress(int counter)
    {
        // do the rolling over account bytes, so each is different but they don't occupy that much memory
        // it's not de Bruijn, but it's as best as possible.
        Keccak key = default;
        BinaryPrimitives.WriteInt32LittleEndian(key.BytesAsSpan, counter);
        return key;
    }

    private static byte[] GetBigAccountValue(int counter) => new UInt256(1, (ulong)counter).ToBigEndian();
}
