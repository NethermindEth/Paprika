using System.Buffers.Binary;
using NUnit.Framework;

namespace Tree.Tests;

public class PageTests
{
    [Test]
    public void Simple()
    {
        using var manager = new MemoryPageManager(1024 * 1024);

        var root = manager.GetClean(out _);

        var key = new byte[32];

        for (byte i = 0; i < byte.MaxValue; i++)
        {
            key[0] = 0x12;
            key[1] = 0x34;
            key[2] = 0x56;
            key[3] = 0x78;
            key[31] = i;
            var path = NibblePath.FromKey(key);
            root = root.Set(path, key, 0, manager);
        }
        
        Console.WriteLine($"Used memory {manager.TotalUsedPages:P}");
    }
    
    [Test]
    public void Random_big()
    {
        using var manager = new MemoryPageManager(1024 * 1024 * 1024UL);

        const int count = 1000;
        
        var root = manager.GetClean(out _);

        var random = new Random(13);

        var key = new byte[32];

        for (int i = 0; i < count; i++)
        {
            random.NextBytes(key);
            var path = NibblePath.FromKey(key);
            root = root.Set(path, key, 0, manager);
        }
        
        // reset random
        random = new Random(13);
        for (int i = 0; i < count; i++)
        {
            random.NextBytes(key);
            var path = NibblePath.FromKey(key);
            Assert.True(root.TryGet(path, out var value, 0, manager));
            Assert.True(value.SequenceEqual(key.AsSpan()));
        }
        
        Console.WriteLine($"Used memory {manager.TotalUsedPages:P}");
    }
    
    [Test]
    public void Same_path()
    {
        using var manager = new MemoryPageManager(128 * 1024 * 1024UL);

        var root = manager.GetClean(out _);

        var key = new byte[32];

        for (int i = 0; i < 1000000; i++)
        {
            BinaryPrimitives.WriteInt32BigEndian(key.AsSpan(28, 4), i);
            var path = NibblePath.FromKey(key);
            root = root.Set(path, key, 0, manager);
        }
        
        Console.WriteLine($"Used memory {manager.TotalUsedPages:P}");
    }

}