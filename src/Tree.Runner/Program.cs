﻿using Tree.Tests;

namespace Tree.Runner;

public static class Program
{
    private const int Count = 160_000_000;
    private const int LogEvery = 10_000_000;
    private const long Gb = 1024 * 1024 * 1024L;
    private const long DbFileSize = 20 * Gb;

    public static void Main(String[] args)
    {
        const int seed = 17;

        var dir = Directory.GetCurrentDirectory();
        var dataPath = Path.Combine(dir, "db");

        if (Directory.Exists(dataPath))
        {
            Directory.Delete(dataPath, true);
        }

        Directory.CreateDirectory(dataPath);

        var manager = new DummyMemoryMappedFilePageManager(DbFileSize, dataPath);
        var root = manager.GetNewDirtyPage(out _);
        root.Clear();

        // var db = new PersistentDb(dataPath);
        // var tree = new PaprikaTree(db);
        //
        byte[] key = new byte[32];
        // ReadOnlySpan<byte> rkey = MemoryMarshal.CreateReadOnlySpan(ref key[0], 32);
        //
        // AnsiConsole.WriteLine($"The test will write {Count} in batches of {BatchSize}.");
        // AnsiConsole.WriteLine($"The time includes the generation of random data which impacts the speed of the test.");
        // AnsiConsole.WriteLine("Running...");
        //
        using (new Measure("Writing", Count))
        {
            var random = new Random(seed);

            for (var i = 0; i < Count; i++)
            {
                random.NextBytes(key);

                root.Set(NibblePath.FromKey(key), key, 0, manager);

                if (i % LogEvery == 0 && i > 0)
                {
                    var used = manager.TotalUsedPages;
                    Console.WriteLine(
                        "Wrote {0:N0} items, DB usage is at {1:P} which gives {2:F2}GB out of allocated {3}GB", i, used,
                        used * DbFileSize / Gb, DbFileSize / Gb);
                }
            }
        }

        using (new Measure("Reading", Count))
        {
            var random = new Random(seed);

            for (var i = 0; i < Count; i++)
            {
                random.NextBytes(key);

                if (!root.TryGet(NibblePath.FromKey(key), out var v, 0, manager))
                {
                    throw new Exception($"Missing value at {i}!");
                }

                if (!v.SequenceEqual(key))
                {
                    throw new Exception($"Wrong value at {i}!");
                }

                if (i % LogEvery == 0)
                {
                    Console.WriteLine("Read {0:N0} items", i);
                }
            }
        }

        // db.PrintStats();
    }
}