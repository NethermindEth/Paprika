using System.Buffers.Binary;
using Spectre.Console;
using Tree.Tests;

namespace Tree.Runner;

public static class Program
{
    public static void Main(String[] args)
    {
        const int DefaultCount = 80_000_000;
        const int BatchSize = 10000;

        var dir = Directory.GetCurrentDirectory();
        var dataPath = Path.Combine(dir, "db");

        if (Directory.Exists(dataPath))
        {
            Directory.Delete(dataPath, true);
        }

        Directory.CreateDirectory(dataPath);

        var db = new PersistentDb(dataPath);

        var tree = new PaprikaTree(db);

        var key = new byte[32];
        var value = new byte[32];

        var count = AnsiConsole.Ask("[green]How many[/] key value pairs should be used for benchmark?",
            DefaultCount);
        var batchSize = AnsiConsole.Ask("What [green]batch size[/] should be used ?", BatchSize);

        using (new Measure("Writing", count, batchSize))
        {
            var batch = tree.Begin();
            for (var i = 0; i < count; i++)
            {
                BinaryPrimitives.WriteInt32LittleEndian(key, i);
                BinaryPrimitives.WriteInt32LittleEndian(value, i);

                batch.Set(key, value);

                if (i % batchSize == 0)
                {
                    batch.Commit();
                    batch = tree.Begin();
                }
            }

            batch.Commit();
        }

        using (new Measure("Reading", count, batchSize))
        {
            for (var i = 0; i < DefaultCount; i++)
            {
                BinaryPrimitives.WriteInt32LittleEndian(key, i);
                tree.TryGet(key, out var v);
            }
        }

        db.PrintStats();
    }
}