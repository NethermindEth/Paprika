using NUnit.Framework;
using Paprika.Crypto;
using Paprika.Data;
using Paprika.Merkle;

namespace Paprika.Tests.Merkle;

public class NodeTests
{
    [Test]
    [TestCase(Node.Type.Branch, true, 0b0000)]
    [TestCase(Node.Type.Branch, false, 0b0101)]
    [TestCase(Node.Type.Leaf, true, 0b0000)]
    [TestCase(Node.Type.Leaf, false, 0b0000)]
    [TestCase(Node.Type.Extension, true, 0b0000)]
    [TestCase(Node.Type.Extension, false, 0b0000)]
    public void Header_properties(Node.Type nodeType, bool isDirty, byte metadata)
    {
        var header = new Node.Header(nodeType, isDirty, metadata);

        Assert.That(header.NodeType, Is.EqualTo(nodeType));
        Assert.That(header.IsDirty, Is.EqualTo(isDirty));
        Assert.That(header.Metadata, Is.EqualTo(metadata));
    }

    [Test]
    [TestCase(Node.Type.Leaf, false, 0b0000)]
    [TestCase(Node.Type.Extension, false, 0b0000)]
    [TestCase(Node.Type.Branch, false, 0b0000)]
    [TestCase(Node.Type.Leaf, true, 0b0000)]
    [TestCase(Node.Type.Extension, true, 0b0000)]
    [TestCase(Node.Type.Branch, true, 0b0001)]
    public void Node_header_read_write(Node.Type nodeType, bool isDirty, byte metadata)
    {
        var header = new Node.Header(nodeType, isDirty, metadata);
        Span<byte> buffer = stackalloc byte[Node.Header.Size];

        var encoded = header.WriteTo(buffer);
        var leftover = Node.Header.ReadFrom(encoded, out var decoded);

        Assert.That(leftover.Length, Is.Zero);
        Assert.That(decoded.Equals(header), $"Expected {header.ToString()}, got {decoded.ToString()}");
    }

    [Test]
    public void Branch_properties()
    {
        ushort nibbles = 0b0000_0000_0000_0011;
        var branch = new Node.Branch(nibbles, Values.Key0);

        Assert.That(branch.Header.NodeType, Is.EqualTo(Node.Type.Branch));
        Assert.That(branch.Header.IsDirty, Is.True);
        Assert.That(branch.Keccak, Is.EqualTo(Values.Key0));
    }

    [Test]
    public void Branch_no_nibbles()
    {
        Assert.Throws<ArgumentException>(() =>
        {
            ushort nibbles = 0b0000_0000_0000_0000;
            _ = new Node.Branch(nibbles, Values.Key0);
        });
    }

    [Test]
    public void Branch_one_nibble()
    {
        for (var i = 0; i < 16; i++)
        {
            Assert.Throws<ArgumentException>(() =>
            {
                ushort nibbles = (ushort)(0b0000_0000_0000_00001 << i);
                _ = new Node.Branch(nibbles, Values.Key0);
            });
        }
    }

    [Test]
    public void Branch_some_nibbles()
    {
        ushort nibbles = 0b0110_1001_0101_1010;
        var branch = new Node.Branch(nibbles, Values.Key0);

        var expected = new byte[] { 1, 3, 4, 6, 8, 11, 13, 14 };

        foreach (var nibble in expected)
        {
            Assert.That(branch.HasNibble(nibble), $"Nibble {nibble} was expected to be set, but it's not");
        }
    }

    [Test]
    public void Branch_no_keccak()
    {
        ushort nibbles = 0b0110_1001_0101_1010;
        var branch = new Node.Branch(nibbles);

        Assert.That(branch.Keccak, Is.EqualTo(Keccak.Zero));
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
        var branch = new Node.Branch(nibbleBitSet, keccak);
        Span<byte> buffer = stackalloc byte[branch.MaxByteLength];

        var encoded = branch.WriteTo(buffer);
        var leftover = Node.Branch.ReadFrom(encoded, out var decoded);

        Assert.That(leftover.Length, Is.Zero);
        Assert.That(decoded.Equals(branch), $"Expected {branch.ToString()}, got {decoded.ToString()}");
    }

    [Test]
    [TestCase((ushort)0b0110_1001_0101_1010)]
    [TestCase((ushort)0b1001_0110_1010_0101)]
    [TestCase((ushort)0b0000_1000_0001_0000)]
    public void Branch_read_write_no_keccak(ushort nibbleBitSet)
    {
        var branch = new Node.Branch(nibbleBitSet);
        Span<byte> buffer = stackalloc byte[branch.MaxByteLength];

        var encoded = branch.WriteTo(buffer);
        var leftover = Node.Branch.ReadFrom(encoded, out var decoded);

        Assert.That(leftover.Length, Is.Zero);
        Assert.That(decoded.Equals(branch), $"Expected {branch.ToString()}, got {decoded.ToString()}");
    }

    [Test]
    public void Branch_no_keccak_encoded_smaller_than_with_keccak()
    {
        ushort nibbles = 0b0110_1001_0101_1010;
        var noKeccak = new Node.Branch(nibbles);
        var hasKeccak = new Node.Branch(nibbles, Values.Key0);

        Span<byte> noKeccakBuffer = stackalloc byte[noKeccak.MaxByteLength];
        var encodedNoKeccak = noKeccak.WriteTo(noKeccakBuffer);

        Span<byte> hasKeccakBuffer = stackalloc byte[hasKeccak.MaxByteLength];
        var encodedHasKeccak = hasKeccak.WriteTo(hasKeccakBuffer);

        Assert.That(noKeccak.MaxByteLength, Is.LessThan(hasKeccak.MaxByteLength));
        Assert.That(encodedNoKeccak.Length, Is.LessThan(encodedHasKeccak.Length));
    }

    [Test]
    public void Leaf_properties()
    {
        var path = NibblePath.FromKey(new byte[] { 0xA, 0x9, 0x6, 0x3 });
        var keccak = Values.Key0;

        var leaf = new Node.Leaf(path, keccak);

        Assert.That(leaf.Header.IsDirty, Is.True);
        Assert.That(leaf.Header.NodeType, Is.EqualTo(Node.Type.Leaf));
        Assert.That(leaf.Path.Equals(path), $"Expected {path.ToString()}, got {leaf.Path.ToString()}");
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
        var leaf = new Node.Leaf(NibblePath.FromKey(pathBytes), keccak);
        Span<byte> buffer = stackalloc byte[leaf.MaxByteLength];

        var encoded = leaf.WriteTo(buffer);
        var leftover = Node.Leaf.ReadFrom(encoded, out var decoded);

        Assert.That(leftover.Length, Is.Zero);
        Assert.That(decoded.Equals(leaf), $"Expected {leaf.ToString()}, got {decoded.ToString()}");
    }

    [Test]
    public void Extension_properties()
    {
        var path = NibblePath.FromKey(new byte[] { 0xA, 0x9, 0x6, 0x3 });

        var extension = new Node.Extension(path);

        Assert.That(extension.Header.IsDirty, Is.True);
        Assert.That(extension.Header.NodeType, Is.EqualTo(Node.Type.Extension));
        Assert.That(extension.Path.Equals(path), $"Expected {path.ToString()}, got {extension.Path.ToString()}");
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
        var extension = new Node.Extension(NibblePath.FromKey(pathBytes));
        Span<byte> buffer = stackalloc byte[extension.MaxByteLength];

        var encoded = extension.WriteTo(buffer);
        var leftover = Node.Extension.ReadFrom(encoded, out var decoded);

        Assert.That(leftover.Length, Is.Zero);
        Assert.That(decoded.Equals(extension), $"Expected {extension.ToString()}, got {decoded.ToString()}");
    }

    [Test]
    public void Node_read_leaf()
    {
        var nibblePath = NibblePath.FromKey(new byte[] { 0x1, 0x2 });
        var keccak = Values.Key0;

        var leaf = new Node.Leaf(nibblePath, keccak);
        Span<byte> buffer = stackalloc byte[leaf.MaxByteLength];

        var encoded = leaf.WriteTo(buffer);
        var leftover = Node.ReadFrom(encoded, out var nodeType, out var actual, out _, out _);

        Assert.That(leftover.Length, Is.Zero);
        Assert.That(nodeType, Is.EqualTo(Node.Type.Leaf));
        Assert.That(actual.Equals(leaf));
    }

    [Test]
    public void Node_read_extension()
    {
        var nibblePath = NibblePath.FromKey(new byte[] { 0x1, 0x2 });

        var extension = new Node.Extension(nibblePath);
        Span<byte> buffer = stackalloc byte[extension.MaxByteLength];

        var encoded = extension.WriteTo(buffer);
        var leftover = Node.ReadFrom(encoded, out var nodeType, out _, out var actual, out _);

        Assert.That(leftover.Length, Is.Zero);
        Assert.That(nodeType, Is.EqualTo(Node.Type.Extension));
        Assert.That(actual.Equals(extension));
    }

    [Test]
    public void Node_read_branch()
    {
        ushort nibbleBitSet = 0b1100_0011_0101_1010;
        var keccak = Values.Key0;

        var branch = new Node.Branch(nibbleBitSet, keccak);
        Span<byte> buffer = stackalloc byte[branch.MaxByteLength];

        var encoded = branch.WriteTo(buffer);
        var leftover = Node.ReadFrom(encoded, out var nodeType, out _, out _, out var actual);

        Assert.That(leftover.Length, Is.Zero);
        Assert.That(nodeType, Is.EqualTo(Node.Type.Branch));
        Assert.That(actual.Equals(branch));
    }

    [Test]
    public void Node_read_sequential()
    {
        var nibblePath = NibblePath.FromKey(new byte[] { 0x1, 0x2, 0x4, 0x5 });
        var nibbleBitSet = (ushort)0b0000_0011;
        var keccak = Values.Key0;

        var leaf = new Node.Leaf(nibblePath, keccak);
        var extension = new Node.Extension(nibblePath);
        var branch = new Node.Branch(nibbleBitSet, keccak);

        Span<byte> buffer = new byte[leaf.MaxByteLength + extension.MaxByteLength + branch.MaxByteLength];

        var writeLeftover = leaf.WriteToWithLeftover(buffer);
        writeLeftover = extension.WriteToWithLeftover(writeLeftover);
        _ = branch.WriteToWithLeftover(writeLeftover);

        var readLeftover = Node.ReadFrom(buffer, out _, out var actualLeaf, out _, out _);
        readLeftover = Node.ReadFrom(readLeftover, out _, out _, out var actualExtension, out _);
        _ = Node.ReadFrom(readLeftover, out _, out _, out _, out var actualBranch);

        Assert.That(actualLeaf.Equals(leaf));
        Assert.That(actualExtension.Equals(extension));
        Assert.That(actualBranch.Equals(branch));
    }

    [Test]
    public void Node_read_invalid_header()
    {
        Assert.Throws<ArgumentOutOfRangeException>(() =>
        {
            Span<byte> header = stackalloc byte[Node.Header.Size];
            header[0] = 0b1111_1111;

            _ = Node.ReadFrom(header, out _, out _, out _, out _);
        });
    }
}
