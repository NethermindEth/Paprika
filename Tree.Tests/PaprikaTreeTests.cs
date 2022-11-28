using System.Buffers.Binary;
using NUnit.Framework;

namespace Tree.Tests;

public class PaprikaTreeTests
{
    [Test]
    public void NonUpdatableTest()
    {
        using var db = new MemoryDb(1024 * 1024 * 1024);

        var tree = new PaprikaTree(db);

        const int count = 1_200_000;

        foreach (var (key, value) in Build(count))
        {
            tree.Set(key.AsSpan(), value.AsSpan());
        }

        foreach (var (key, value) in Build(count))
        {
            Assert.True(tree.TryGet(key.AsSpan(), out var retrieved), $"for key {key.Field0}");
            Assert.True(retrieved.SequenceEqual(value.AsSpan()));
        }

        var percentage = (int)(((double)db.Position) / db.Size * 100);

        Console.WriteLine($"used {percentage}%");
    }
    
    [Test]
    public void UpdatableTest()
    {
        using var db = new MemoryDb(1024 * 1024 * 1024);

        var tree = new PaprikaTree(db);

        const int count = 1_200_000;

        int i = 0;
        int batchSize = 10000;
        
        var batch = tree.Begin();
        foreach (var (key, value) in Build(count))
        {
            batch.Set(key.AsSpan(), value.AsSpan());
            i++;
            if (i > batchSize)
            {
                batch.Commit();
                batch = tree.Begin();
                i = 0;
            }
        }
        
        batch.Commit();

        foreach (var (key, value) in Build(count))
        {
            Assert.True(tree.TryGet(key.AsSpan(), out var retrieved), $"for key {key.Field0}");
            Assert.True(retrieved.SequenceEqual(value.AsSpan()));
        }

        var percentage = (int)(((double)db.Position) / db.Size * 100);

        Console.WriteLine($"used {percentage}%");
    }

    private static IEnumerable<KeyValuePair<Keccak, Keccak>> Build(int number)
    {
        // builds the values so no extensions in the tree are required
        for (long i = 1; i < number + 1; i++)
        {
            // set nibbles, so that no extensions happen
            // var n = (int)(((i & 0xF) << NibbleSize) |
            //               ((i & 0xF0) >> NibbleSize) |
            //               ((i & 0xF00) << NibbleSize) |
            //               ((i & 0xF000) >> NibbleSize) |
            //               ((i & 0xF0000) << NibbleSize) |
            //               ((i & 0xF00000) >> NibbleSize) |
            //               ((i & 0xF000000) << NibbleSize) |
            //               ((i & 0xF0000000) >> NibbleSize));

            var n = i;

            Keccak key = default;
            Keccak value = default;

            BinaryPrimitives.WriteInt32LittleEndian(key.AsSpan(), (int)n);
            BinaryPrimitives.WriteInt32LittleEndian(value.AsSpan(), (int)i);

            yield return new KeyValuePair<Keccak, Keccak>(key, value);
        }
    }
}