using System.Reflection.Emit;
using NUnit.Framework;
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

    private static int GetTypeSize(Type type)
    {
        var dm = new DynamicMethod("$", typeof(int), Type.EmptyTypes);
        ILGenerator il = dm.GetILGenerator();
        il.Emit(OpCodes.Sizeof, type);
        il.Emit(OpCodes.Ret);

        return (int)(dm.Invoke(null, null) ?? -1);
    }
}
