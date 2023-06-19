using NUnit.Framework;
using Paprika.Crypto;
using Paprika.Data;
using Paprika.Merkle;

namespace Paprika.Tests.Merkle;

public class NodeTest
{
    [Test]
    public void Branch_encoding()
    {
        var branch = new Branch(0b0110_0110_1001_1001);

        var expectedNibbles = new List<byte> { 0x0, 0x3, 0x4, 0x7, 0x9, 0xA, 0xD, 0xE };
        Assert.That(branch.Nibbles(), Is.EquivalentTo(expectedNibbles));

        // TODO: Test without unsafe
        unsafe
        {
            Assert.That(sizeof(Branch), Is.EqualTo(35));
        }

        Assert.That(branch.IsDirty, Is.EqualTo(true));
        Assert.That(branch.Type, Is.EqualTo(NodeType.Branch));
        Assert.That(branch.Keccak, Is.EqualTo(Keccak.Zero));
    }


    [Test]
    public void Extension_encoding()
    {
        var rawPath = new byte[] { 0x1, 0x2, 0x3 };
        var nibblePath = new NibblePath(rawPath, 0, rawPath.Length);
        var extension = new Extension(nibblePath);

        // TODO: Figure how to store var-length encoded `NibblePath`
        // Assert.That(extension.Path.Equals(nibblePath));

        // TODO: Test without unsafe
        unsafe
        {
            Assert.That(sizeof(Extension), Is.EqualTo(1));
        }

        Assert.That(extension.IsDirty, Is.EqualTo(true));
        Assert.That(extension.Type, Is.EqualTo(NodeType.Extension));
    }


    [Test]
    public void Leaf_encoding()
    {
        var rawPath = new byte[] { 0x2, 0x4, 0x6 };
        var nibblePath = new NibblePath(rawPath, 0, rawPath.Length);
        var leaf = new LeafNode(nibblePath);

        // TODO: Figure how to store var-length encoded `NibblePath`
        // Assert.That(leaf.Path.Equals(nibblePath));

        // TODO: Test without unsafe
        unsafe
        {
            Assert.That(sizeof(LeafNode), Is.EqualTo(33));
        }

        Assert.That(leaf.IsDirty, Is.EqualTo(true));
        Assert.That(leaf.Type, Is.EqualTo(NodeType.Leaf));
    }
}
