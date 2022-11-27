using System.Buffers.Binary;
using Tree.Tests.Mocks;

namespace Tree.Tests;

public static class Program
{
    public static void Main(String[] args)
    {
        using var db = new TestMemoryDb((int)(1.9 * 1024 * 1024 * 1024));

        var tree = new PaprikaTree(db);

        const int count = 2_600_000;

        var key = new byte[32];
        var value = new byte[32];

        for (var i = 0; i < count; i++)
        {
            BinaryPrimitives.WriteInt32LittleEndian(key, i);
            BinaryPrimitives.WriteInt32LittleEndian(value, i);
            
            tree.Set(key, value);
        }

        for (var i = 0; i < count; i++)
        {
            BinaryPrimitives.WriteInt32LittleEndian(key, i);
            tree.TryGet(key, out var v);
        }
        
        Console.WriteLine("executed!");
    }
}