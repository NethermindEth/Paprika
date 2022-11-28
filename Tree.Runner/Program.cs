using System.Buffers.Binary;
using System.Diagnostics;

namespace Tree.Tests;

public static class Program
{
    private const int Count = 15_000_000;
    
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

        var write = Stopwatch.StartNew();
        for (var i = 0; i < Count; i++)
        {
            BinaryPrimitives.WriteInt32LittleEndian(key, i);
            BinaryPrimitives.WriteInt32LittleEndian(value, i);
            
            tree.Set(key, value);
        }
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
        Console.WriteLine($"{name} of {Count} items took {sw.Elapsed.ToString()}");
    }
}