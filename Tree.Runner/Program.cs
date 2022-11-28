using System.Buffers.Binary;
using System.Diagnostics;

namespace Tree.Tests;

public static class Program
{
    private const int Count = 50_000_000;
    private const int BatchSize = 10000;
    
    public static void Main(String[] args)
    {
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

        var batch = tree.Begin();

        var write = Stopwatch.StartNew();
        for (var i = 0; i < Count; i++)
        {
            BinaryPrimitives.WriteInt32LittleEndian(key, i);
            BinaryPrimitives.WriteInt32LittleEndian(value, i);
            
            batch.Set(key, value);
            
            if (i % BatchSize == 0)
            {
                batch.Commit();
                batch = tree.Begin();
            }
        }
        
        batch.Commit();
        
        Stop(write, "Writing");
        
        var read = Stopwatch.StartNew();
        for (var i = 0; i < Count; i++)
        {
            BinaryPrimitives.WriteInt32LittleEndian(key, i);
            tree.TryGet(key, out var v);
        }
        Stop(read, "Reading");
        
        db.PrintStats();
    }

    private static void Stop(Stopwatch sw, string name)
    {
        var throughput = (int)(Count / sw.Elapsed.TotalSeconds);
        Console.WriteLine($"{name} of {Count:N} items with batch of {BatchSize} took {sw.Elapsed.ToString()} giving a throughput {throughput:N} items/s");
    }
}