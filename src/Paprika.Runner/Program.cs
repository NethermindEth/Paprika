using System.Buffers.Binary;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Runtime.InteropServices;
using Paprika.Crypto;
using Paprika.Db;

[assembly: ExcludeFromCodeCoverage]

namespace Paprika.Runner;

public static class Program
{
    private const int BlockCount = 4_000;
    private const int AccountCount = 260_000_000;
    private const int AccountsPerBlock = 1000;

    private const int RandomSeed = 17;

    private const int LogEveryNBlocks = 100;

    private const long DbFileSize = 16 * Gb;
    private const long Gb = 1024 * 1024 * 1024L;
    private const CommitOptions Commit = CommitOptions.FlushDataOnly;

    public static void Main(String[] args)
    {
        var dir = Directory.GetCurrentDirectory();
        var dataPath = Path.Combine(dir, "db");

        if (Directory.Exists(dataPath))
        {
            Console.WriteLine("Deleting previous db...");
            Directory.Delete(dataPath, true);
        }

        Directory.CreateDirectory(dataPath);

        Console.WriteLine("Initializing db of size {0}GB", DbFileSize / Gb);
        Console.WriteLine("Starting benchmark with commit level {0}", Commit);

        var db = new MemoryMappedPagedDb(DbFileSize, 64, dataPath);

        var accountsBytes = PrepareAccounts();

        var counter = 0;

        // writing
        var writing = Stopwatch.StartNew();

        for (uint block = 0; block < BlockCount; block++)
        {
            using var batch = db.BeginNextBlock();

            for (var account = 0; account < AccountsPerBlock; account++)
            {
                var key = GetAccountKey(accountsBytes, counter);

                batch.Set(key, new Account(block, block));
                counter++;
            }

            if (block > 0 & block % LogEveryNBlocks == 0)
            {
                Console.WriteLine(
                    $"At block: {block,4} with speed {TimeSpan.FromTicks(writing.ElapsedTicks / block)}/block");
            }

            batch.Commit(Commit);
        }

        Console.WriteLine("Writing state of {0} accounts per block through {1} blocks, generated {2} accounts, took {3} used {4:F2}GB",
            AccountsPerBlock, BlockCount, counter, writing.Elapsed, db.ActualMegabytesOnDisk / 1024);

        // reading
        var reading = Stopwatch.StartNew();
        using var read = db.BeginReadOnlyBatch();

        for (var account = 0; account < counter; account++)
        {
            var key = GetAccountKey(accountsBytes, counter);
            read.GetAccount(key);
        }

        Console.WriteLine("Reading state of all of {0} accounts from the last block took {1}",
            counter, reading.Elapsed);
    }

    private static Keccak GetAccountKey(Span<byte> accountsBytes, int counter)
    {
        // do the rolling over account bytes, so each is different but they don't occupy that much memory
        // it's not de Bruijn, but it's as best as possible.

        Keccak key = default;
        accountsBytes.Slice(counter, Keccak.Size).CopyTo(key.BytesAsSpan);
        return key;
    }

    private static unsafe Span<byte> PrepareAccounts()
    {
        Console.WriteLine("Preparing random accounts addresses...");
        var accounts = new Span<byte>(NativeMemory.Alloc((UIntPtr)AccountCount), AccountCount);
        new Random(RandomSeed).NextBytes(accounts);
        Console.WriteLine("Accounts prepared");
        return accounts;
    }
}