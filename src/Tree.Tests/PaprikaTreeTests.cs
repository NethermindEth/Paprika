using System.Buffers.Binary;
using System.Runtime.InteropServices;
using NUnit.Framework;

namespace Tree.Tests;

public class PaprikaTreeTests
{
    [Test]
    public void Extension()
    {
        using var db = new MemoryDb(64 * 1024);

        var tree = new PaprikaTree(db);

        var key1 = new byte[32];
        key1[0] = 0x01;
        key1[1] = 0x02;
        key1[31] = 0xA;

        var key2 = new byte[32];
        key2[0] = 0x01;
        key2[1] = 0x03;
        key2[31] = 0xB;

        var key3 = new byte[32];
        key3[0] = 0x01;
        key3[1] = 0x04;
        key3[31] = 0xC;

        var key4 = new byte[32];
        key4[0] = 0x11; // split on the extension 2nd nibble
        key4[1] = 0x05;
        key4[31] = 0xD;

        var key5 = new byte[32];
        key5[0] = 0x00; // split on the extension 1st nibble
        key5[1] = 0x06;
        key5[31] = 0xE;

        var batch = tree.Begin();
        batch.Set(key1, key1);
        batch.Set(key2, key2);
        batch.Set(key3, key3);
        batch.Set(key4, key4);
        batch.Set(key5, key5);
        batch.Commit();

        AssertTree(tree, key1);
        AssertTree(tree, key2);
        AssertTree(tree, key3);
        AssertTree(tree, key4);
        AssertTree(tree, key5);

        void AssertTree(PaprikaTree paprikaTree, byte[] bytes)
        {
            Assert.True(paprikaTree.TryGet(bytes.AsSpan(), out var retrieved));
            Assert.True(retrieved.SequenceEqual(bytes.AsSpan()));
        }
    }

    [Test]
    public void Branching_Full()
    {
        using var db = new MemoryDb(64 * 1024);

        var tree = new PaprikaTree(db);

        var keys = Enumerable.Range(0, 16).Select(i =>
            {
                var key = new byte[32];
                key[0] = (byte)(i << 4);
                return key;
            }
        ).ToArray();

        var batch = tree.Begin();
        foreach (var key in keys)
        {
            batch.Set(key, key);
        }

        batch.Commit();
        
        foreach (var key in keys)
        {
            AssertTree(tree, key);
        }
        
        void AssertTree(PaprikaTree paprikaTree, byte[] bytes)
        {
            Assert.True(paprikaTree.TryGet(bytes.AsSpan(), out var retrieved));
            Assert.True(retrieved.SequenceEqual(bytes.AsSpan()));
        }
    }

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

        const int count = 50;

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

    [Test]
    public void VariousCases()
    {
        const int seed = 13;
        using var db = new MemoryDb(1024 * 1024 * 1024);

        var tree = new PaprikaTree(db);

        const int count = 1_200_000;
        const int batchSize = 10000;

        int i = 0;

        Span<byte> key = stackalloc byte[32];
        ReadOnlySpan<byte> rkey = MemoryMarshal.CreateReadOnlySpan(ref key[0], 32);
        Span<byte> value = stackalloc byte[32];

        var random = new Random(seed);

        var batch = tree.Begin();

        for (var item = 0; item < count; item++)
        {
            random.NextBytes(key);
            random.NextBytes(value);
            batch.Set(key, value);
            i++;
            if (i > batchSize)
            {
                batch.Commit();
                batch = tree.Begin();
                i = 0;
            }
        }

        batch.Commit();

        // reinitialize
        random = new Random(seed);

        for (var item = 0; item < count; item++)
        {
            random.NextBytes(key);
            random.NextBytes(value);

            Assert.True(tree.TryGet(rkey, out var retrieved), $"for item number {item}");
            Assert.True(retrieved.SequenceEqual(value), $"for item number {item}");
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