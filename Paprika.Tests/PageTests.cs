using System.Buffers.Binary;
using NUnit.Framework;
using Paprika.Db;

namespace Paprika.Tests;

public class PageTests
{
    [Test]
    public void Simple()
    {
        using var db = new NativeMemoryPagedDb(1024 * 1024UL);
        var tx = db.Begin();

        var key = new byte[32];

        for (byte i = 0; i < byte.MaxValue; i++)
        {
            key[0] = 0x12;
            key[1] = 0x34;
            key[2] = 0x56;
            key[3] = 0x78;
            key[31] = i;

            tx.Set(key, key);
        }

        Console.WriteLine($"Used memory {db.TotalUsedPages:P}");
    }

    [Test]
    public void Random_big()
    {
        using var db = new NativeMemoryPagedDb(1024 * 1024 * 1024UL);
        var tx = db.Begin();

        const int count = 1000_000;

        var random = new Random(13);

        var key = new byte[32];

        for (int i = 0; i < count; i++)
        {
            random.NextBytes(key);
            tx.Set(key, key);

            AssertValue(tx, key, i);
        }
        
        tx.Commit();

        // reset random
        random = new Random(13);
        for (int i = 0; i < count; i++)
        {
            random.NextBytes(key);
            AssertValue(tx, key, i);
        }

        static void AssertValue(ITransaction tx, byte[] key, int i)
        {
            Assert.True(tx.TryGet(key, out var value), $"Failed getting  at {i}");
            Assert.True(value.SequenceEqual(key.AsSpan()));
        }
    }

    [Test]
    [Ignore("Currently updates in place are not supported")]
    public void Same_path()
    {
        using var db = new NativeMemoryPagedDb(1024 * 1024UL);
        var tx = db.Begin();

        var key = new byte[32];

        for (int i = 0; i < 1000000; i++)
        {
            BinaryPrimitives.WriteInt32BigEndian(key.AsSpan(28, 4), i);
            tx.Set(key, key);
        }

        Console.WriteLine($"Used memory {db.TotalUsedPages:P}");
    }
}