﻿// #define PERSISTENT_DB
#define STORAGE

using System.Buffers.Binary;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Runtime.InteropServices;
using HdrHistogram;
using Nethermind.Int256;
using Paprika.Crypto;
using Paprika.Db;

[assembly: ExcludeFromCodeCoverage]

namespace Paprika.Runner;

public static class Program
{
    private const int BlockCount = 50_000;
    private const int RandomSampleSize = 260_000_000;
    private const int AccountsPerBlock = 1000;

    private const int RandomSeed = 17;

    private const int NumberOfLogs = 10;

    private const long DbFileSize = 18 * Gb;
    private const long Gb = 1024 * 1024 * 1024L;
    private const CommitOptions Commit = CommitOptions.FlushDataOnly;
    private const int LogEvery = BlockCount / NumberOfLogs;

    public static void Main(String[] args)
    {
#if PERSISTENT_DB
        var dir = Directory.GetCurrentDirectory();
        var dataPath = Path.Combine(dir, "db");

        if (Directory.Exists(dataPath))
        {
            Console.WriteLine("Deleting previous db...");
            Directory.Delete(dataPath, true);
        }

        Directory.CreateDirectory(dataPath);
        Console.WriteLine("Using persistent DB");
#else
        Console.WriteLine("Using in-memory DB for greater speed.");
#endif

        Console.WriteLine("Initializing db of size {0}GB", DbFileSize / Gb);
        Console.WriteLine("Starting benchmark with commit level {0}", Commit);

        var histograms = new
        {
            allocated = new IntHistogram(short.MaxValue, 3),
            reused = new IntHistogram(short.MaxValue, 3),
            total = new IntHistogram(short.MaxValue, 3),
        };

#if PERSISTENT_DB
        var db = new MemoryMappedPagedDb(DbFileSize, 64, dataPath, metrics =>
#else
        var db = new NativeMemoryPagedDb(DbFileSize, 64, metrics =>
#endif
        {
            histograms.allocated.RecordValue(metrics.PagesAllocated);
            histograms.reused.RecordValue(metrics.PagesReused);
            histograms.total.RecordValue(metrics.TotalPagesWritten);
        });

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


        // writing
        var writing = Stopwatch.StartNew();

        for (uint block = 0; block < BlockCount; block++)
        {
            using var batch = db.BeginNextBlock();

            for (var account = 0; account < AccountsPerBlock; account++)
            {
                var key = GetAccountKey(random, counter);

                batch.Set(key, GetAccountValue(counter));
#if STORAGE               
                var storage = GetStorageAddress(counter);
                var storageValue = GetStorageValue(counter);
                batch.SetStorage(key, storage, storageValue);
#endif
                counter++;
            }

            batch.Commit(Commit);

            if (block > 0 & block % LogEvery == 0)
            {
                ReportProgress(block, writing);
                writing.Restart();
            }
        }

        ReportProgress(BlockCount - 1, writing);

        Console.WriteLine();
        Console.WriteLine(
            "Writing state of {0} accounts per block, each with 1 storage, through {1} blocks, generated {2} accounts, used {3:F2}GB",
            AccountsPerBlock, BlockCount, counter, db.ActualMegabytesOnDisk / 1024);

        // reading
        Console.WriteLine();
        Console.WriteLine("Reading and asserting values...");

        var reading = Stopwatch.StartNew();
        using var read = db.BeginReadOnlyBatch();

        for (var i = 0; i < counter; i++)
        {
            var key = GetAccountKey(random, i);
            var a = read.GetAccount(key);

            if (a != GetAccountValue(i))
            {
                throw new InvalidOperationException($"Invalid account state for account {i}!");
            }
#if STORAGE 
            var storage = GetStorageAddress(i);
            var actualStorage = read.GetStorage(key, storage);
            var expectedStorage = GetStorageValue(i);
            if (actualStorage != expectedStorage)
            {
                throw new InvalidOperationException($"Invalid storage for account number {i}!");
            }
#endif
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
                $"{db.ActualMegabytesOnDisk / 1024:F2}GB",
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

    private static unsafe Span<byte> PrepareStableRandomSource()
    {
        Console.WriteLine("Preparing random accounts addresses...");
        var accounts = new Span<byte>(NativeMemory.Alloc((UIntPtr)RandomSampleSize), RandomSampleSize);
        new Random(RandomSeed).NextBytes(accounts);
        Console.WriteLine("Accounts prepared");
        return accounts;
    }
}