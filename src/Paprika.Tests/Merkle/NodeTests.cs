using FluentAssertions;
using NUnit.Framework;
using Paprika.Crypto;
using Paprika.Data;
using Paprika.Merkle;

namespace Paprika.Tests.Merkle;

public class NodeTests
{
    [Test]
    [TestCase(Node.Type.Branch, 0b0000)]
    [TestCase(Node.Type.Branch, 0b0101)]
    [TestCase(Node.Type.Leaf, 0b0000)]
    [TestCase(Node.Type.Extension, 0b0000)]
    public void Header_properties(Node.Type nodeType, byte metadata)
    {
        var header = new Node.Header(nodeType, metadata);

        header.NodeType.Should().Be(nodeType);
        header.Metadata.Should().Be(metadata);
    }

    [Test]
    [TestCase(Node.Type.Leaf, 0b0000)]
    [TestCase(Node.Type.Leaf, 0b0001)]
    [TestCase(Node.Type.Extension, 0b0000)]
    [TestCase(Node.Type.Extension, 0b0001)]
    [TestCase(Node.Type.Branch, 0b0000)]
    [TestCase(Node.Type.Branch, 0b0001)]
    public void Node_header_read_write(Node.Type nodeType, byte metadata)
    {
        var header = new Node.Header(nodeType, metadata);
        Span<byte> buffer = stackalloc byte[Node.Header.Size];

        var encoded = header.WriteTo(buffer);
        var leftover = Node.Header.ReadFrom(encoded, out var decoded);

        leftover.Length.Should().Be(0);
        decoded.Should().Be(header, $"Expected {header.ToString()}, got {decoded.ToString()}");
    }

    [Test]
    public void Branch_properties()
    {
        ushort nibbles = 0b0000_0000_0000_0011;
        var branch = new Node.Branch(new NibbleSet.Readonly(nibbles), Values.Key0);

        branch.Header.NodeType.Should().Be(Node.Type.Branch);
        branch.Keccak.Should().Be(Values.Key0);
    }

    [Test]
    public void Branch_no_nibbles()
    {
        Assert.Throws<ArgumentException>(() =>
        {
            ushort nibbles = 0b0000_0000_0000_0000;
            _ = new Node.Branch(new NibbleSet.Readonly(nibbles), Values.Key0);
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
                _ = new Node.Branch(new NibbleSet.Readonly(nibbles), Values.Key0);
            });
        }
    }

    [Test]
    public void Branch_some_nibbles()
    {
        ushort nibbles = 0b0110_1001_0101_1010;
        var branch = new Node.Branch(new NibbleSet.Readonly(nibbles), Values.Key0);

        var expected = new byte[] { 1, 3, 4, 6, 8, 11, 13, 14 };

        foreach (var nibble in expected)
        {
            branch.Children[nibble].Should().BeTrue($"Nibble {nibble} was expected to be set, but it's not");
        }
    }

    [Test]
    public void Branch_no_keccak()
    {
        ushort nibbles = 0b0110_1001_0101_1010;
        var branch = new Node.Branch(new NibbleSet.Readonly(nibbles));

        branch.Keccak.Should().Be(Keccak.Zero);
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
        var branch = new Node.Branch(new NibbleSet.Readonly(nibbleBitSet), keccak);
        Span<byte> buffer = stackalloc byte[branch.MaxByteLength];

        var encoded = branch.WriteTo(buffer);
        var leftover = Node.Branch.ReadFrom(encoded, out var decoded);

        leftover.Length.Should().Be(0);
        decoded.Equals(branch).Should().BeTrue($"Expected {branch.ToString()}, got {decoded.ToString()}");
    }

    [Test]
    [TestCase((ushort)0b0110_1001_0101_1010)]
    [TestCase((ushort)0b1001_0110_1010_0101)]
    [TestCase((ushort)0b0000_1000_0001_0000)]
    public void Branch_read_write_no_keccak(ushort nibbleBitSet)
    {
        var branch = new Node.Branch(new NibbleSet.Readonly(nibbleBitSet), default(Keccak));
        Span<byte> buffer = stackalloc byte[branch.MaxByteLength];

        var encoded = branch.WriteTo(buffer);
        var leftover = Node.Branch.ReadFrom(encoded, out var decoded);

        leftover.Length.Should().Be(0);
        decoded.Equals(branch).Should().BeTrue($"Expected {branch.ToString()}, got {decoded.ToString()}");
    }

    [Test]
    public void Branch_no_keccak_encoded_smaller_than_with_keccak()
    {
        const ushort nibbles = 0b0110_1001_0101_1010;
        var noKeccak = new Node.Branch(new NibbleSet.Readonly(nibbles));
        var hasKeccak = new Node.Branch(new NibbleSet.Readonly(nibbles), Values.Key0);

        Span<byte> noKeccakBuffer = stackalloc byte[noKeccak.MaxByteLength];
        var encodedNoKeccak = noKeccak.WriteTo(noKeccakBuffer);

        Span<byte> hasKeccakBuffer = stackalloc byte[hasKeccak.MaxByteLength];
        var encodedHasKeccak = hasKeccak.WriteTo(hasKeccakBuffer);

        noKeccak.MaxByteLength.Should().BeLessThan(hasKeccak.MaxByteLength);
        encodedNoKeccak.Length.Should().BeLessThan(encodedHasKeccak.Length);
    }

    [Test]
    public void Leaf_properties()
    {
        var path = NibblePath.FromKey(new byte[] { 0xA, 0x9, 0x6, 0x3 });

        var leaf = new Node.Leaf(path);

        leaf.Header.NodeType.Should().Be(Node.Type.Leaf);
        leaf.Path.Equals(path).Should().BeTrue($"Expected {path.ToString()}, got {leaf.Path.ToString()}");
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
        var leaf = new Node.Leaf(NibblePath.FromKey(pathBytes));
        Span<byte> buffer = stackalloc byte[leaf.MaxByteLength];

        var encoded = leaf.WriteTo(buffer);
        var leftover = Node.Leaf.ReadFrom(encoded, out var decoded);

        leftover.Length.Should().Be(0);
        decoded.Equals(leaf).Should().BeTrue($"Expected {leaf.ToString()}, got {decoded.ToString()}");
    }

    [Test]
    public void Extension_properties()
    {
        var path = NibblePath.FromKey(new byte[] { 0xA, 0x9, 0x6, 0x3 });

        var extension = new Node.Extension(path);

        extension.Header.NodeType.Should().Be(Node.Type.Extension);
        extension.Path.Equals(path).Should().BeTrue($"Expected {path.ToString()}, got {extension.Path.ToString()}");
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

        leftover.Length.Should().Be(0);
        decoded.Equals(extension).Should().BeTrue($"Expected {extension.ToString()}, got {decoded.ToString()}");
    }

    [Test]
    public void Node_read_leaf()
    {
        var nibblePath = NibblePath.FromKey(new byte[] { 0x1, 0x2 });

        var leaf = new Node.Leaf(nibblePath);
        Span<byte> buffer = stackalloc byte[leaf.MaxByteLength];

        var encoded = leaf.WriteTo(buffer);
        var leftover = Node.ReadFrom(encoded, out var nodeType, out var actual, out _, out _);

        leftover.Length.Should().Be(0);
        nodeType.Should().Be(Node.Type.Leaf);
        actual.Equals(leaf).Should().BeTrue();
    }

    [Test]
    public void Node_read_extension()
    {
        var nibblePath = NibblePath.FromKey(new byte[] { 0x1, 0x2 });

        var extension = new Node.Extension(nibblePath);
        Span<byte> buffer = stackalloc byte[extension.MaxByteLength];

        var encoded = extension.WriteTo(buffer);
        var leftover = Node.ReadFrom(encoded, out var nodeType, out _, out var actual, out _);

        leftover.Length.Should().Be(0);
        nodeType.Should().Be(Node.Type.Extension);
        actual.Equals(extension).Should().BeTrue();
    }

    [Test]
    public void Node_read_branch()
    {
        const ushort nibbleBitSet = 0b1100_0011_0101_1010;
        var keccak = Values.Key0;

        var branch = new Node.Branch(new NibbleSet.Readonly(nibbleBitSet), keccak);
        Span<byte> buffer = stackalloc byte[branch.MaxByteLength];

        var encoded = branch.WriteTo(buffer);
        var leftover = Node.ReadFrom(encoded, out var nodeType, out _, out _, out var actual);

        leftover.Length.Should().Be(0);
        nodeType.Should().Be(Node.Type.Branch);
        actual.Equals(branch).Should().BeTrue();
    }

    [Test]
    public void Node_read_sequential()
    {
        var nibblePath = NibblePath.FromKey(new byte[] { 0x1, 0x2, 0x4, 0x5 });
        const ushort nibbleBitSet = 0b0000_0011;
        var keccak = Values.Key0;

        var leaf = new Node.Leaf(nibblePath);
        var extension = new Node.Extension(nibblePath);
        var branch = new Node.Branch(new NibbleSet.Readonly(nibbleBitSet), keccak);

        Span<byte> buffer = new byte[leaf.MaxByteLength + extension.MaxByteLength + branch.MaxByteLength];

        var writeLeftover = leaf.WriteToWithLeftover(buffer);
        writeLeftover = extension.WriteToWithLeftover(writeLeftover);
        _ = branch.WriteToWithLeftover(writeLeftover);

        var readLeftover = Node.ReadFrom(buffer, out _, out var actualLeaf, out _, out _);
        readLeftover = Node.ReadFrom(readLeftover, out _, out _, out var actualExtension, out _);
        _ = Node.ReadFrom(readLeftover, out _, out _, out _, out var actualBranch);

        actualLeaf.Equals(leaf).Should().BeTrue();
        actualExtension.Equals(extension).Should().BeTrue();
        actualBranch.Equals(branch).Should().BeTrue();
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