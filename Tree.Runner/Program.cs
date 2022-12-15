using System.Runtime.InteropServices;
using Spectre.Console;
using Tree.Tests;

namespace Tree.Runner;

public static class Program
{
    public static void Main(String[] args)
    {
        const int Count = 80_000_000;
        const int BatchSize = 10000;
        const int seed = 17;
        
        var dir = Directory.GetCurrentDirectory();
        var dataPath = Path.Combine(dir, "db");

        if (Directory.Exists(dataPath))
        {
            Directory.Delete(dataPath, true);
        }

        Directory.CreateDirectory(dataPath);

        var db = new PersistentDb(dataPath);

        var tree = new PaprikaTree(db);

        Span<byte> key = stackalloc byte[32];
        ReadOnlySpan<byte> rkey = MemoryMarshal.CreateReadOnlySpan(ref key[0], 32);
        Span<byte> value = stackalloc byte[32];

        AnsiConsole.WriteLine($"The test will write [green]{Count} items[/] in [green]batches of {BatchSize}[/].");
        AnsiConsole.WriteLine($"The time includes the generation of random data which impacts the speed of the test.");
        AnsiConsole.WriteLine("Running...");
        
        using (new Measure("Writing", Count, BatchSize))
        {
            var random = new Random(seed);
            
            var batch = tree.Begin();
            for (var i = 0; i < Count; i++)
            {
                random.NextBytes(key);
                random.NextBytes(value);

                batch.Set(key, value);

                if (i % BatchSize == 0)
                {
                    batch.Commit();
                    batch = tree.Begin();
                }
            }

            batch.Commit();
        }

        using (new Measure("Reading", Count, BatchSize))
        {
            var random = new Random(seed);
            
            for (var i = 0; i < Count; i++)
            {
                random.NextBytes(key);
                random.NextBytes(value);
                
                tree.TryGet(in rkey, out var v);
            }
        }

        db.PrintStats();
    }
}