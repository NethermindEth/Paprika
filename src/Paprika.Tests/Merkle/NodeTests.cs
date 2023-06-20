using System.Reflection.Emit;
using NUnit.Framework;
using Paprika.Crypto;
using Paprika.Merkle;

namespace Paprika.Tests.Merkle;

public class NodeTests
{
    [Test]
    [TestCase(typeof(MerkleNodeHeader), 1)]
    [TestCase(typeof(Branch), 35)]
    public void Struct_size(Type type, int expectedSize)
    {
        Assert.That(GetTypeSize(type), Is.EqualTo(expectedSize));
    }

    [Test]
    [TestCase(NodeType.Branch, true)]
    [TestCase(NodeType.Branch, false)]
    [TestCase(NodeType.Leaf, true)]
    [TestCase(NodeType.Leaf, false)]
    [TestCase(NodeType.Extension, true)]
    [TestCase(NodeType.Extension, false)]
    public void Node_header_data(NodeType nodeType, bool isDirty)
    {
        var header = new MerkleNodeHeader(nodeType, isDirty);

        Assert.That(header.NodeType, Is.EqualTo(nodeType));
        Assert.That(header.IsDirty, Is.EqualTo(isDirty));
    }

    [Test]
    [TestCase( NodeType.Leaf, false, new byte[]{ 0b0000 })]
    [TestCase( NodeType.Extension, false, new byte[]{ 0b0010 })]
    [TestCase( NodeType.Branch, false, new byte[]{ 0b0100 })]
    [TestCase(NodeType.Leaf, true, new byte[]{ 0b0001 })]
    [TestCase(NodeType.Extension, true, new byte[]{ 0b0011 })]
    [TestCase(NodeType.Branch, true, new byte[]{ 0b0101 })]
    public void Node_header_read_write(NodeType nodeType, bool isDirty, byte[] encoded)
    {
        _ = MerkleNodeHeader.ReadFrom(encoded, out var header);
        Assert.That(header.IsDirty, Is.EqualTo(isDirty));
        Assert.That(header.NodeType, Is.EqualTo(nodeType));

        Span<byte> buffer = stackalloc byte[MerkleNodeHeader.MaxSize];
        _ = header.WriteTo(buffer);
        Assert.That(buffer.SequenceEqual(encoded));
    }

    [Test]
    public void Branch_properties()
    {
        ushort nibbles = 0b0000_0000_0000_0000;
        var branch = new Branch(nibbles, Values.Key0);

        Assert.That(branch.NodeType, Is.EqualTo(NodeType.Branch));
        Assert.That(branch.IsDirty, Is.EqualTo(true));
        Assert.That(branch.Keccak, Is.EqualTo(Values.Key0));
    }

    [Test]
    public void Branch_no_nibbles()
    {
        ushort nibbles = 0b0000_0000_0000_0000;
        var branch = new Branch(nibbles, Values.Key0);

        for (byte nibble = 0; nibble < 16; nibble++)
        {
            Assert.That(branch.HasNibble(nibble), Is.Not.True);
        }
    }

    [Test]
    public void Branch_some_nibbles()
    {
        ushort nibbles = 0b0110_1001_0101_1010;
        var branch = new Branch(nibbles, Values.Key0);

        var expected = new byte[] { 1, 3, 4, 6, 8, 11, 13, 14 };

        foreach (var nibble in expected)
        {
            Assert.That(branch.HasNibble(nibble), $"Nibble {nibble} was expected to be set, but it's not");
        }
    }

    private static object[] _branchReadWriteCases = {
        new object[] { (ushort) 0b0110_1001_0101_1010, new byte[] { 1, 3, 4, 6, 8, 11, 13, 14 },Values.Key0 },
        new object[] { (ushort) 0b1001_0110_1010_0101, new byte[] { 0, 2, 5, 7, 9, 10, 12, 15 },Values.Key1A },
        new object[] { (ushort) 0b0000_1000_0001_0000, new byte[] { 4, 11 },Values.Key1B },
    };

    [Test]
    [TestCaseSource(nameof(_branchReadWriteCases))]
    public void Branch_read_write(ushort nibbleBitSet, byte[] nibbles, Keccak keccak)
    {
        Span<byte> encoded = stackalloc byte[Branch.MaxSize];
        // Header
        encoded[0] = 0b0101;
        // Nibble BitSet
        BitConverter.TryWriteBytes(encoded.Slice(1), nibbleBitSet);
        // Keccak
        keccak.Span.CopyTo(encoded.Slice(3));

        _ = Branch.ReadFrom(encoded, out var branch);
        Assert.That(branch.IsDirty, Is.True);
        Assert.That(branch.Keccak, Is.EqualTo(keccak));
        foreach (var nibble in nibbles)
        {
            Assert.That(branch.HasNibble(nibble), $"Nibble {nibble} was expected to be set, but it's not");
        }

        Span<byte> buffer = stackalloc byte[Branch.MaxSize];
        _ = branch.WriteTo(buffer);

        Assert.That(buffer.SequenceEqual(encoded));
    }

    private static int GetTypeSize(Type type)
    {
        var dm = new DynamicMethod("$", typeof(int), Type.EmptyTypes);
        ILGenerator il = dm.GetILGenerator();
        il.Emit(OpCodes.Sizeof, type);
        il.Emit(OpCodes.Ret);

        return (int)(dm.Invoke(null, null) ?? -1);
    }
}
