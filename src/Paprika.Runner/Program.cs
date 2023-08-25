using System.Buffers.Binary;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Reflection;
using System.Text;
using HdrHistogram;
using Nethermind.Int256;
using Paprika.Chain;
using Paprika.Crypto;
using Paprika.Merkle;
using Paprika.Store;
using Paprika.Tests;
using Spectre.Console;
using Spectre.Console.Rendering;

[assembly: ExcludeFromCodeCoverage]

namespace Paprika.Runner;

/// <summary>
/// The case for running the runner.
/// </summary>
public record Case(uint BlockCount, int AccountsPerBlock, ulong DbFileSize, bool PersistentDb, TimeSpan FlushEvery,
    bool Fsync,
    bool UseBigStorageAccount)
{
    public uint NumberOfLogs => PersistentDb ? 100u : 10u;
    public uint LogEvery => BlockCount / NumberOfLogs;
}

public static class Program
{
    private static readonly Case InMemoryReallySmall =
        new(5_000, 1000, 1 * Gb, false, TimeSpan.FromSeconds(5), false, false);

    private static readonly Case InMemorySmall =
        new(50_000, 1000, 11 * Gb, false, TimeSpan.FromSeconds(5), false, false);
    private static readonly Case
        InMemoryBig = new(100_000, 1000, 48 * Gb, false, TimeSpan.FromSeconds(5), false, false);
    private static readonly Case DiskSmallNoFlush =
        new(50_000, 1000, 11 * Gb, true, TimeSpan.FromSeconds(5), false, false);
    private static readonly Case DiskSmallFlushFile =
        new(50_000, 1000, 11 * Gb, true, TimeSpan.FromSeconds(5), true, false);

    private const int MaxReorgDepth = 64;
    private const int FinalizeEvery = 32;

    private const int RandomSeed = 17;
    private const long Gb = 1024 * 1024 * 1024L;
    private const int UseStorageEveryNAccounts = 10;
    private const int BigStorageAccountSlotCount = 1_000_000;

    public static async Task Main(String[] args)
    {
        // select the case
        var caseName = (args.Length > 0 ? args[0] : nameof(InMemoryReallySmall)).ToLowerInvariant();
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

            //var merkle = new ComputeMerkleBehavior(true, 2, 2);
            IPreCommitBehavior preCommit = null;

            await using (var blockchain = new Blockchain(db, preCommit, config.FlushEvery, 1000, reporter.Observe))
            {
                counter = Writer(config, blockchain, bigStorageAccount, random, layout[writing]);
            }

            // waiting for finalization
            using var read = db.BeginReadOnlyBatch("Runner - reading");

            var readingStopWatch = Stopwatch.StartNew();
            random = BuildRandom();

            var logReadEvery = counter / config.NumberOfLogs;
            for (var i = 0; i < counter; i++)
            {
                var key = random.NextKeccak();
                var actual = read.GetAccount(key);

                var expected = GetAccountValue(i);

                if (actual != expected)
                {
                    throw new InvalidOperationException(
                        $"Invalid account state for account number {i} with address {key.ToString()}. " +
                        $"The expected value is {expected} while the actual is {actual}!");
                }

                if (UseStorageEveryNAccounts > 0 && i % UseStorageEveryNAccounts == 0)
                {
                    var storageAddress = GetStorageAddress(i);
                    var expectedStorageValue = GetStorageValue(i);
                    read.AssertStorageValue(key, storageAddress, expectedStorageValue);
                }

                if (config.UseBigStorageAccount)
                {
                    var index = i % BigStorageAccountSlotCount;
                    var storageAddress = GetStorageAddress(index);
                    var expectedStorageValue = GetBigAccountValue(index);
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
            layout[info].Update(new Panel("Gathering statistics...").Header("Paprika tree statistics").Expand());

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
                    WriteHistogram(level.ChildCount),
                    WriteHistogram(level.Entries));
            }

            var mb = (long)stats.PageCount * Page.PageSize / 1024 / 1024;

            var types = string.Join(", ", stats.PageTypes.Select(kvp => $"{kvp.Key}: {kvp.Value}"));

            // histogram description
            var sb = new StringBuilder();
            sb.Append("Histogram percentiles: ");
            foreach (var percentile in Percentiles)
            {
                sb.Append($"[{percentile.color}]P{percentile.value}: {percentile.value}th percentile [/] ");
            }

            var report = new Layout()
                .SplitRows(
                    new Layout(
                            new Rows(
                                new Markup(sb.ToString()),
                                new Text(""),
                                new Text("General stats:"),
                                new Text($"1. Size of this Paprika tree: {mb}MB"),
                                new Text($"2. Types of pages: {types}"),
                                WriteHistogram(stats.PageAge, "2. Age of pages: ")))
                        .Size(7),
                    new Layout(table.Expand()));

            layout[info].Update(new Panel(report).Header("Paprika tree statistics").Expand());

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

    private static IRenderable WriteHistogram(HistogramBase histogram, string prefix = "")
    {
        string Percentile(int percentile, string color)
        {
            var value = histogram.GetValueAtPercentile(percentile);
            return $"[{color}]P{percentile}: {value,2}[/] ";
        }

        var sb = new StringBuilder();

        sb.Append(prefix);
        foreach (var percentile in Percentiles)
        {
            sb.Append(Percentile(percentile.value, percentile.color));
        }

        return new Markup(sb.ToString());
    }

    private static readonly (int value, string color)[] Percentiles =
    {
        new(50, "green"),
        new(90, "yellow"),
        new(95, "red"),
    };

    private static int Writer(Case config, Blockchain blockchain, Keccak bigStorageAccount, Random random,
        Layout reporting)
    {
        var counter = 0;

        bool bigStorageAccountCreated = false;

        // writing
        var writing = Stopwatch.StartNew();
        var parentBlockHash = Keccak.Zero;

        var toFinalize = new List<Keccak>();

        for (uint block = 1; block < config.BlockCount; block++)
        {
            var blockHash = Keccak.Compute(parentBlockHash.Span);
            using var worldState = blockchain.StartNew(parentBlockHash, blockHash, block);

            parentBlockHash = blockHash;

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

            if (block > 0 & block % config.LogEvery == 0)
            {
                reporting.Update(
                    new Panel($@"At block {block}. Writing last batch took {writing.Elapsed:g}").Header("Writing")
                        .Expand());
                writing.Restart();
            }
        }

        // flush leftovers by adding one more block for now
        var lastBlock = toFinalize.Last();
        using var placeholder = blockchain.StartNew(lastBlock, Keccak.Compute(lastBlock.Span), config.BlockCount);
        placeholder.Commit();
        blockchain.Finalize(lastBlock);

        reporting.Update(
            new Panel($@"At block {config.BlockCount - 1}. Writing last batch took {writing.Elapsed:g}")
                .Header("Writing")
                .Expand());


        return counter;
    }

    private static Account GetAccountValue(int counter)
    {
        return new Account((UInt256)counter, (UInt256)counter);
    }

    private static byte[] GetStorageValue(int counter) => ((UInt256)counter + 100000).ToBigEndian();

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

    private static byte[] GetBigAccountValue(int counter) =>
        (new UInt256((ulong)(counter * 2246822519U), (ulong)(counter * 374761393U))).ToBigEndian();
}