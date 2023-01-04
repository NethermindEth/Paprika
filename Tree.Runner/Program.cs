using System.Runtime.InteropServices;
using Spectre.Console;
using Tree.Crypto;
using Tree.Tests;

namespace Tree.Runner;

public static class Program
{
    public static void Main(String[] args)
    {
        const int Count = 160_000_000;
        const int LogEvery = 1000_000;
        const int seed = 17;
        
        var dir = Directory.GetCurrentDirectory();
        var dataPath = Path.Combine(dir, "db");

        if (Directory.Exists(dataPath))
        {
            Directory.Delete(dataPath, true);
        }

        Directory.CreateDirectory(dataPath);

        var manager = new DummyMemoryMappedFilePageManager(30 * 1024 * 1024 * 1024L, dataPath);
        var root = manager.GetClean(out _);
        
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
        using (new Measure("Writing", Count, Count))
        {
            var random = new Random(seed);
            
            for (var i = 0; i < Count; i++)
            {
                random.NextBytes(key);

                root.Set(NibblePath.FromKey(key), key, 0, manager);
                
                if (i % LogEvery == 0 && i > 0)
                {
                    Console.WriteLine("Wrote {0:N0} items. Db usage is at {1:P}", i, manager.TotalUsedPages);
                }
            }
        }
        //
        // using (new Measure("Reading", Count, BatchSize))
        // {
        //     var random = new Random(seed);
        //     
        //     for (var i = 0; i < Count; i++)
        //     {
        //         random.NextBytes(key);
        //         
        //         tree.TryGet(in rkey, out var v);
        //         
        //         if (i % LogEvery == 0)
        //         {
        //             Console.WriteLine("Read {0:N0} items", i);
        //         }
        //     }
        // }
        //
        // db.PrintStats();
    }
}