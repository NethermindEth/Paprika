using System.Buffers.Binary;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using Nethermind.Int256;
using Paprika.Chain;
using Paprika.Crypto;
using Paprika.Store;
using Paprika.Tests;
using Spectre.Console;

[assembly: ExcludeFromCodeCoverage]

namespace Paprika.Runner;

public static class Program
{
    private const int BlockCount = PersistentDb ? 25_000 : 3_000;
    private const int AccountsPerBlock = 1000;
    private const int MaxReorgDepth = 64;
    private const int FinalizeEvery = 32;

    private const int RandomSeed = 17;

    private const int NumberOfLogs = PersistentDb ? 100 : 10;

    private const long DbFileSize = PersistentDb ? 256 * Gb : 16 * Gb;
    private const long Gb = 1024 * 1024 * 1024L;

    private static readonly TimeSpan FlushEvery = TimeSpan.FromSeconds(5);

    private const int LogEvery = BlockCount / NumberOfLogs;

    private const bool PersistentDb = true;
    private const int UseStorageEveryNAccounts = 10;
    private const bool UseBigStorageAccount = false;
    private const int BigStorageAccountSlotCount = 1_000_000;

    public static async Task Main(String[] args)
    {
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

            if (PersistentDb)
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

            Console.WriteLine("Initializing db of size {0}GB", DbFileSize / Gb);
            Console.WriteLine("Starting benchmark. Flush buffer every: {0}ms", ((int)FlushEvery.TotalMilliseconds).ToString());

            PagedDb db = PersistentDb
                ? PagedDb.MemoryMappedDb(DbFileSize, MaxReorgDepth, dataPath)
                : PagedDb.NativeMemoryDb(DbFileSize, MaxReorgDepth);

            var random = BuildRandom();
            var bigStorageAccount = GetBigAccountKey();

            Console.WriteLine();
            Console.WriteLine("Writing:");
            Console.WriteLine("- {0} accounts per block through {1} blocks", AccountsPerBlock, BlockCount);
            Console.WriteLine("- it gives {0} total accounts", AccountsPerBlock * BlockCount);

            if (UseStorageEveryNAccounts > 0)
            {
                Console.WriteLine($"- every {UseStorageEveryNAccounts}th account will have 1 storage slot written");
            }

            if (UseBigStorageAccount)
            {
                Console.WriteLine("- each account amends 1 slot in Big Storage account");
            }

            var counter = 0;

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

            await using (var blockchain = new Blockchain(db, null, FlushEvery, 1000, reporter.Observe))
            {
                counter = Writer(blockchain, bigStorageAccount, random, layout[writing]);
            }

            // waiting for finalization
            using var read = db.BeginReadOnlyBatch();

            var readingStopWatch = Stopwatch.StartNew();
            random = BuildRandom();

            var logReadEvery = counter / NumberOfLogs;
            for (var i = 0; i < counter; i++)
            {
                var key = random.NextKeccak();
                var actual = read.GetAccount(key);

                var expected = GetAccountValue(i);

                if (actual != expected)
                {
                    throw new InvalidOperationException($"Invalid account state for account number {i} with address {key.ToString()}. " +
                                                        $"The expected value is {expected} while the actual is {actual}!");
                }

                if (UseStorageEveryNAccounts > 0 && i % UseStorageEveryNAccounts == 0)
                {
                    var storageAddress = GetStorageAddress(i);
                    var expectedStorageValue = GetStorageValue(i);
                    var actualStorage = read.GetStorage(key, storageAddress);

                    if (actualStorage != expectedStorageValue)
                    {
                        throw new InvalidOperationException($"Invalid storage for account number {i}!");
                    }
                }

                if (UseBigStorageAccount)
                {
                    var index = i % BigStorageAccountSlotCount;
                    var storageAddress = GetStorageAddress(index);
                    var expectedStorageValue = GetBigAccountValue(index);
                    var actualStorage = read.GetStorage(bigStorageAccount, storageAddress);

                    if (actualStorage != expectedStorageValue)
                    {
                        throw new InvalidOperationException($"Invalid storage for big storage account at index {i}!");
                    }
                }

                if (i > 0 & i % logReadEvery == 0)
                {
                    ReportReading(i);
                }
            }

            // the final report
            ReportReading(counter);

            var stats = new StatisticsReporter();
            read.Report(stats);
            var table = new Table();

            table.AddColumn(new TableColumn("Level of Paprika tree"));
            table.AddColumn(new TableColumn("Child page count"));
            table.AddColumn(new TableColumn("Entries in page"));

            foreach (var (key, level) in stats.Levels)
            {
                table.AddRow(
                    new Text(key.ToString()),
                    new Text(level.ChildCount.GetValueAtPercentile(90).ToString()),
                    new Text(level.Entries.GetValueAtPercentile(90).ToString()));
            }

            layout[info].Update(new Panel(table.Expand()).Header("Paprika tree statistics").Expand());

            spectre.Cancel();
            await reportingTask;

            void ReportReading(int i)
            {
                var secondsPerRead = TimeSpan.FromTicks(readingStopWatch.ElapsedTicks / logReadEvery).TotalSeconds;
                var readsPerSeconds = 1 / secondsPerRead;

                var txt = $"Reading at {i,9} out of {counter} accounts. Current speed: {readsPerSeconds:F1} reads/s";

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

    private static int Writer(Blockchain blockchain, Keccak bigStorageAccount, Random random,
        Layout reporting)
    {
        var counter = 0;

        bool bigStorageAccountCreated = false;

        // writing
        var writing = Stopwatch.StartNew();
        var parentBlockHash = Keccak.Zero;

        var toFinalize = new List<Keccak>();

        for (uint block = 1; block < BlockCount; block++)
        {
            var blockHash = Keccak.Compute(parentBlockHash.Span);
            using var worldState = blockchain.StartNew(parentBlockHash, blockHash, block);

            parentBlockHash = blockHash;

            for (var account = 0; account < AccountsPerBlock; account++)
            {
                var key = random.NextKeccak();

                worldState.SetAccount(key, GetAccountValue(counter));

                if (UseStorageEveryNAccounts > 0 && counter % UseStorageEveryNAccounts == 0)
                {
                    var storageAddress = GetStorageAddress(counter);
                    var storageValue = GetStorageValue(counter);
                    worldState.SetStorage(key, storageAddress, storageValue);
                }

                if (UseBigStorageAccount)
                {
                    if (bigStorageAccountCreated == false)
                    {
                        worldState.SetAccount(bigStorageAccount, new Account(100, 100));
                        bigStorageAccountCreated = true;
                    }

                    var index = counter % BigStorageAccountSlotCount;
                    var storageAddress = GetStorageAddress(index);
                    var storageValue = GetBigAccountValue(counter);

                    worldState.SetStorage(bigStorageAccount, storageAddress, storageValue);
                }

                counter++;
            }

            worldState.Commit();

            // finalize
            if (toFinalize.Count >= FinalizeEvery)
            {
                // finalize first
                blockchain.Finalize(toFinalize[0]);
                toFinalize.Clear();
            }

            toFinalize.Add(blockHash);

            if (block > 0 & block % LogEvery == 0)
            {
                reporting.Update(
                    new Panel($@"At block {block}. Writing last batch took {writing.Elapsed:g}").Header("Writing")
                        .Expand());
                writing.Restart();
            }
        }

        // flush leftovers by adding one more block for now
        var lastBlock = toFinalize.Last();
        using var placeholder = blockchain.StartNew(lastBlock, Keccak.Compute(lastBlock.Span), BlockCount);
        placeholder.Commit();
        blockchain.Finalize(lastBlock);

        reporting.Update(
            new Panel($@"At block {BlockCount - 1}. Writing last batch took {writing.Elapsed:g}").Header("Writing")
                .Expand());


        return counter;
    }

    private static Account GetAccountValue(int counter)
    {
        return new Account((UInt256)counter, (UInt256)counter);
    }

    private static UInt256 GetStorageValue(int counter) => (UInt256)counter + 100000;

    private static Keccak GetBigAccountKey()
    {
        Keccak key = default;
        var random = new Random(17);
        random.NextBytes(key.BytesAsSpan);
        return key;
    }

    private static Keccak GetStorageAddress(int counter)
    {
        // do the rolling over account bytes, so each is different but they don't occupy that much memory
        // it's not de Bruijn, but it's as best as possible.
        Keccak key = default;
        BinaryPrimitives.WriteInt32LittleEndian(key.BytesAsSpan, counter);
        return key;
    }

    private static UInt256 GetBigAccountValue(int counter) =>
        new((ulong)(counter * 2246822519U), (ulong)(counter * 374761393U));
}