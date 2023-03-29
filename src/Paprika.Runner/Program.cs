using System.Diagnostics;
using Paprika.Db;

namespace Paprika.Runner;

public static class Program
{
    private const int Count = 160_000_000;
    private const int LogEvery = 10_000_000;
    private const long Gb = 1024 * 1024 * 1024L;
    private const long DbFileSize = 20 * Gb;

    public static void Main(String[] args)
    {
        // const int seed = 17;
        //
        // var dir = Directory.GetCurrentDirectory();
        // var dataPath = Path.Combine(dir, "db");
        //
        // if (Directory.Exists(dataPath))
        // {
        //     Directory.Delete(dataPath, true);
        // }
        //
        // Directory.CreateDirectory(dataPath);
        //
        // var db = new MemoryMappedPagedDb(DbFileSize, dataPath, true);
        // var tx = db.Begin();
        //
        // MeasureThroughput(tx, tx =>
        // {
        //     var key = new byte[32];
        //     var random = new Random(seed);
        //
        //     for (var i = 0; i < Count; i++)
        //     {
        //         random.NextBytes(key);
        //
        //         tx.Set(key, key);
        //
        //         if (i % LogEvery == 0 && i > 0)
        //         {
        //             var used = tx.TotalUsedPages;
        //             Console.WriteLine(
        //                 "Wrote {0:N0} items, DB usage is at {1:P} which gives {2:F2}GB out of allocated {3}GB", i, used,
        //                 used * DbFileSize / Gb, DbFileSize / Gb);
        //         }
        //     }
        // }, "Writing", Count);
        //
        // Measure(tx, tx => tx.Commit(), "Committing to disk (including the root)");
        //
        // MeasureThroughput(db, db =>
        // {
        //     var key = new byte[32];
        //     var random = new Random(seed);
        //
        //     tx = db.Begin();
        //
        //     for (var i = 0; i < Count; i++)
        //     {
        //         random.NextBytes(key);
        //
        //         if (!tx.TryGet(key, out var v))
        //         {
        //             throw new Exception($"Missing value at {i}!");
        //         }
        //
        //         // if (!v.SequenceEqual(key))
        //         // {
        //         //     throw new Exception($"Wrong value at {i}!");
        //         // }
        //
        //         if (i % LogEvery == 0 && i > 0)
        //         {
        //             Console.WriteLine("Read {0:N0} items", i);
        //         }
        //     }
        // }, "Reading", Count);
    }

    private static void MeasureThroughput<TState>(TState t, Action<TState> action, string name, int count)
    {
        var sw = Stopwatch.StartNew();
        try
        {
            action(t);
        }
        finally
        {
            var elapsed = sw.Elapsed;
            var throughput = (int)(count / elapsed.TotalSeconds);
            Console.WriteLine(
                $"{name} of {count:N} items took {elapsed.ToString()} giving a throughput {throughput:N} items/s");
        }
    }

    private static void Measure<TState>(TState t, Action<TState> action, string name)
    {
        var sw = Stopwatch.StartNew();
        try
        {
            action(t);
        }
        finally
        {
            Console.WriteLine();
            Console.WriteLine($"{name} took {sw.Elapsed.ToString()}");
        }
    }
}