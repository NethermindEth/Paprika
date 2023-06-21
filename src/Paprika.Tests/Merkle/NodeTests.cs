using System.Reflection.Emit;
using NUnit.Framework;
using Paprika.Crypto;
using Paprika.Data;
using Paprika.Merkle;

namespace Paprika.Tests.Merkle;

public class NodeTests
{
    [Test]
    [TestCase(typeof(MerkleNodeHeader), 1)]
    [TestCase(typeof(Branch), 35)]
    [TestCase(typeof(Leaf), 64)]
    [TestCase(typeof(Extension), 32)]
    public void Struct_size(Type type, int expectedSize)
    {
        Assert.That(GetSizeOfType(type), Is.EqualTo(expectedSize));
    }

    [Test]
    [TestCase(NodeType.Branch, true)]
    [TestCase(NodeType.Branch, false)]
    [TestCase(NodeType.Leaf, true)]
    [TestCase(NodeType.Leaf, false)]
    [TestCase(NodeType.Extension, true)]
    [TestCase(NodeType.Extension, false)]
    public void Node_header_properties(NodeType nodeType, bool isDirty)
    {
        var header = new MerkleNodeHeader(nodeType, isDirty);

        Assert.That(header.NodeType, Is.EqualTo(nodeType));
        Assert.That(header.IsDirty, Is.EqualTo(isDirty));
    }

    [Test]
    [TestCase(NodeType.Leaf, false)]
    [TestCase(NodeType.Extension, false)]
    [TestCase(NodeType.Branch, false)]
    [TestCase(NodeType.Leaf, true)]
    [TestCase(NodeType.Extension, true)]
    [TestCase(NodeType.Branch, true)]
    public void Node_header_read_write(NodeType nodeType, bool isDirty)
    {
        var expected = new MerkleNodeHeader(nodeType, isDirty);
        Span<byte> buffer = stackalloc byte[MerkleNodeHeader.Size];

        _ = expected.WriteTo(buffer);
        _ = MerkleNodeHeader.ReadFrom(buffer, out var actual);

        Assert.That(actual.Equals(expected), $"Expected {expected.ToString()}, got {actual.ToString()}");
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

    private static object[] _branchReadWriteCases =
    {
        new object[] { (ushort)0b0110_1001_0101_1010, Values.Key0 },
        new object[] { (ushort)0b1001_0110_1010_0101, Values.Key1A },
        new object[] { (ushort)0b0000_1000_0001_0000, Values.Key1B },
    };

    [Test]
    [TestCaseSource(nameof(_branchReadWriteCases))]
    public void Branch_read_write(ushort nibbleBitSet, Keccak keccak)
    {
        var branch = new Branch(nibbleBitSet, keccak);

        Span<byte> encoded = stackalloc byte[Branch.MaxByteLength];
        _ = branch.WriteTo(encoded);
        _ = Branch.ReadFrom(encoded, out var decoded);

        Assert.That(decoded.Equals(branch), $"Expected {branch.ToString()}, got {decoded.ToString()}");
    }

    [Test]
    public void Leaf_properties()
    {
        ReadOnlySpan<byte> bytes = new byte[] { 0xA, 0x9, 0x6, 0x3 };
        var path = NibblePath.FromKey(bytes);
        var keccak = Values.Key0;

        var leaf = new Leaf(path, keccak);

        Assert.That(leaf.IsDirty, Is.True);
        Assert.That(leaf.NodeType, Is.EqualTo(NodeType.Leaf));
        Assert.That(leaf.NibblePath.Equals(path), $"Expected {path.ToString()}, got {leaf.NibblePath.ToString()}");
        Assert.That(leaf.Keccak, Is.EqualTo(keccak));
    }

    private static object[] _leafReadWriteCases =
    {
        new object[] { new byte[] { 0x1, 0x2 }, Values.Key0 },
        new object[] { new byte[] { 0xA, 0xB, 0xC, 0xD }, Values.Key1A },
        new object[] { new byte[] { 0xB, 0xC, 0xD, 0xE }, Values.Key1B },
        new object[] { new byte[] { 0x2, 0x4, 0x6, 0x8, 0xA, 0xC, 0xE }, Values.Key0 },
    };

    [Test]
    [TestCaseSource(nameof(_leafReadWriteCases))]
    public void Leaf_read_write(byte[] pathBytes, Keccak keccak)
    {
        var leaf = new Leaf(NibblePath.FromKey(pathBytes), keccak);

        Span<byte> encoded = stackalloc byte[leaf.MaxByteLength];
        _ = leaf.WriteTo(encoded);
        _ = Leaf.ReadFrom(encoded, out var decoded);

        Assert.That(decoded.Equals(leaf), $"Expected {leaf.ToString()}, got {leaf.ToString()}");
    }


    [Test]
    public void Extension_properties()
    {
        ReadOnlySpan<byte> bytes = new byte[] { 0xA, 0x9, 0x6, 0x3 };
        var path = NibblePath.FromKey(bytes);

        var extension = new Extension(path);

        Assert.That(extension.IsDirty, Is.True);
        Assert.That(extension.NodeType, Is.EqualTo(NodeType.Extension));
        Assert.That(extension.NibblePath.Equals(path), $"Expected {path.ToString()}, got {extension.NibblePath.ToString()}");
    }

    private static object[] _extensionReadWriteCases =
    {
        new object[] { new byte[] { 0x0, 0x0 } },
        new object[] { new byte[] { 0xD, 0xC, 0xB, 0xA } },
        new object[] { new byte[] { 0xC, 0xB, 0xA, 0xF } },
        new object[] { Enumerable.Repeat((byte)0xF, 32).ToArray() },
    };

    [Test]
    [TestCaseSource(nameof(_extensionReadWriteCases))]
    public void Extension_read_write(byte[] pathBytes)
    {
        var extension = new Extension(NibblePath.FromKey(pathBytes));

        Span<byte> encoded = stackalloc byte[extension.MaxByteLength];
        _ = extension.WriteTo(encoded);
        _ = Extension.ReadFrom(encoded, out var decoded);

        Assert.That(decoded.Equals(extension), $"Expected {extension.ToString()}, got {extension.ToString()}");
    }

    private static int GetSizeOfType(Type type)
    {
        var dm = new DynamicMethod("$", typeof(int), Type.EmptyTypes);
        ILGenerator il = dm.GetILGenerator();
        il.Emit(OpCodes.Sizeof, type);
        il.Emit(OpCodes.Ret);

        return (int)(dm.Invoke(null, null) ?? -1);
    }
}
