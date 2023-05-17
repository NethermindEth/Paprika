using System.Buffers.Binary;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using HdrHistogram;
using Nethermind.Int256;
using Paprika.Crypto;
using Paprika.Store;

[assembly: ExcludeFromCodeCoverage]

namespace Paprika.Runner;

public static class Program
{
    private const int BlockCount = PersistentDb ? 100_000 : 20_000;
    private const int RandomSampleSize = 260_000_000;
    private const int AccountsPerBlock = 1000;
    private const int MaxReorgDepth = 64;

    private const int RandomSeed = 17;

    private const int NumberOfLogs = PersistentDb ? 100 : 10;

    private const long DbFileSize = PersistentDb ? 128 * Gb : 10 * Gb;
    private const long Gb = 1024 * 1024 * 1024L;

    private const CommitOptions Commit = CommitOptions.FlushDataOnly;

    private const int LogEvery = BlockCount / NumberOfLogs;

    private const bool PersistentDb = true;
    private const bool UseStorage = true;
    private const bool UseBigStorageAccount = true;
    private const int BigStorageAccountSlotCount = 1_000_000;
    private static readonly UInt256[] BigStorageAccountValues = new UInt256[BigStorageAccountSlotCount];

    public static async Task Main(String[] args)
    {
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
        Console.WriteLine("Starting benchmark with commit level {0}", Commit);

        var histograms = new
        {
            allocated = new IntHistogram(short.MaxValue, 3),
            reused = new IntHistogram(short.MaxValue, 3),
            total = new IntHistogram(short.MaxValue, 3),
        };

        void OnMetrics(IBatchMetrics metrics)
        {
            histograms.allocated.RecordValue(metrics.PagesAllocated);
            histograms.reused.RecordValue(metrics.PagesReused);
            histograms.total.RecordValue(metrics.TotalPagesWritten);
        }

        PagedDb db = PersistentDb
            ? PagedDb.MemoryMappedDb(DbFileSize, MaxReorgDepth, dataPath, OnMetrics)
            : PagedDb.NativeMemoryDb(DbFileSize, MaxReorgDepth, OnMetrics);

        var random = PrepareStableRandomSource();

        var counter = 0;

        Console.WriteLine();
        Console.WriteLine("(P) - 90th percentile of the value");
        Console.WriteLine();

        PrintHeader("At Block",
            "Avg. speed",
            "Space used",
            "New pages(P)",
            "Pages reused(P)",
            "Total pages(P)");


        var bigStorageAccount = GetAccountKey(random, RandomSampleSize - Keccak.Size);

        bool bigStorageAccountCreated = false;

        // writing
        var writing = Stopwatch.StartNew();

        for (uint block = 0; block < BlockCount; block++)
        {
            using var batch = db.BeginNextBlock();

            for (var account = 0; account < AccountsPerBlock; account++)
            {
                var key = GetAccountKey(random, counter);

                batch.Set(key, GetAccountValue(counter));

                if (UseStorage)
                {
                    var storageAddress = GetStorageAddress(counter);
                    var storageValue = GetStorageValue(counter);
                    batch.SetStorage(key, storageAddress, storageValue);
                }

                if (UseBigStorageAccount)
                {
                    if (bigStorageAccountCreated == false)
                    {
                        batch.Set(bigStorageAccount, new Account(100, 100));
                        bigStorageAccountCreated = true;
                    }

                    var index = counter % BigStorageAccountSlotCount;
                    var storageAddress = GetStorageAddress(index);
                    var storageValue = GetBigAccountStorageValue(counter);
                    BigStorageAccountValues[index] = storageValue;

                    batch.SetStorage(bigStorageAccount, storageAddress, storageValue);
                }

                counter++;
            }

            await batch.Commit(Commit);

            if (block > 0 & block % LogEvery == 0)
            {
                ReportProgress(block, writing);
                writing.Restart();
            }
        }

        ReportProgress(BlockCount - 1, writing);

        Console.WriteLine();
        Console.WriteLine("Writing in numbers:");
        Console.WriteLine("- {0} accounts per block", AccountsPerBlock);
        if (UseStorage)
        {
            Console.WriteLine("- each account with 1 storage slot written");
        }

        if (UseBigStorageAccount)
        {
            Console.WriteLine("- each account amends 1 slot in Big Storage account");
        }

        Console.WriteLine("- through {0} blocks ", BlockCount);
        Console.WriteLine("- generated accounts total number: {0} ", counter);
        Console.WriteLine("- space used: {0:F2}GB ", db.Megabytes / 1024);

        // reading
        Console.WriteLine();
        Console.WriteLine("Reading and asserting values...");

        var reading = Stopwatch.StartNew();
        using var read = db.BeginReadOnlyBatch();

        var logReadEvery = counter / NumberOfLogs;
        for (var i = 0; i < counter; i++)
        {
            var key = GetAccountKey(random, i);
            var a = read.GetAccount(key);

            if (a != GetAccountValue(i))
            {
                throw new InvalidOperationException($"Invalid account state for account {i}!");
            }

            if (UseStorage)
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
                var expectedStorageValue = BigStorageAccountValues[index];
                var actualStorage = read.GetStorage(bigStorageAccount, storageAddress);

                if (actualStorage != expectedStorageValue)
                {
                    throw new InvalidOperationException($"Invalid storage for big storage account at index {i}!");
                }
            }

            if (i > 0 & i % logReadEvery == 0)
            {
                var secondsPerRead = TimeSpan.FromTicks(reading.ElapsedTicks / logReadEvery).TotalSeconds;
                var readsPerSeconds = 1 / secondsPerRead;

                Console.WriteLine($"Reading at {i,9} out of {counter} accounts. Current speed: {readsPerSeconds:F1} reads/s");
                writing.Restart();
            }
        }

        Console.WriteLine("Reading state of all of {0} accounts from the last block took {1}",
            counter, reading.Elapsed);

        Console.WriteLine("90th percentiles:");
        Write90Th(histograms.allocated, "new pages allocated");
        Write90Th(histograms.reused, "pages reused allocated");
        Write90Th(histograms.total, "total pages written");

        void ReportProgress(uint block, Stopwatch sw)
        {
            var secondsPerBlock = TimeSpan.FromTicks(sw.ElapsedTicks / LogEvery).TotalSeconds;
            var blocksPerSecond = 1 / secondsPerBlock;

            PrintRow(
                block.ToString(),
                $"{blocksPerSecond:F1} blocks/s",
                $"{db.Megabytes / 1024:F2}GB",
                $"{histograms.reused.GetValueAtPercentile(90)}",
                $"{histograms.allocated.GetValueAtPercentile(90)}",
                $"{histograms.total.GetValueAtPercentile(90)}");
        }
    }

    private const string Separator = " | ";
    private const int Padding = 15;

    private static void PrintHeader(params string[] values)
    {
        Console.Out.WriteLine(string.Join(Separator, values.Select(v => v.PadRight(Padding))));
    }

    private static void PrintRow(params string[] values)
    {
        Console.Out.WriteLine(string.Join(Separator, values.Select(v => v.PadLeft(Padding))));
    }

    private static Account GetAccountValue(int counter)
    {
        return new Account((UInt256)counter, (UInt256)counter);
    }

    private static UInt256 GetStorageValue(int counter) => (UInt256)counter + 100000;

    private static UInt256 GetBigAccountStorageValue(int counter) => (UInt256)counter + 123456;

    private static void Write90Th(HistogramBase histogram, string name)
    {
        Console.WriteLine($"   - {name} per block: {histogram.GetValueAtPercentile(0.9)}");
        histogram.Reset();
    }

    private static Keccak GetAccountKey(Span<byte> accountsBytes, int counter)
    {
        // do the rolling over account bytes, so each is different but they don't occupy that much memory
        // it's not de Bruijn, but it's as best as possible.

        Keccak key = default;
        accountsBytes.Slice(counter, Keccak.Size).CopyTo(key.BytesAsSpan);
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

    private static byte[] PrepareStableRandomSource()
    {
        Console.WriteLine("Preparing random accounts addresses...");
        var accounts = GC.AllocateArray<byte>(RandomSampleSize);
        new Random(RandomSeed).NextBytes(accounts);
        Console.WriteLine("Accounts prepared");
        return accounts;
    }
}