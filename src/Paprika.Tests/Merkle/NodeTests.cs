using System.Reflection.Emit;
using System.Runtime.InteropServices;
using NUnit.Framework;
using Paprika.Merkle;

namespace Paprika.Tests.Merkle;

public class NodeTests
{
    [Test]
    public void Node_header_size()
    {
        var expectedSizeInBytes = 1;
        Assert.That(Marshal.SizeOf<MerkleNodeHeader>(), Is.EqualTo(expectedSizeInBytes));
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
    public void Branch_size()
    {
        Assert.That(GetTypeSize(typeof(Branch)), Is.EqualTo(35));
    }

    [Test]
    public void Branch_no_nibbles()
    {
        ushort nibbles = 0b0000_0000_0000_0000;
        var branch = new Branch(nibbles, Values.Key0);

        Assert.That(branch.NodeType, Is.EqualTo(NodeType.Branch));
        Assert.That(branch.IsDirty, Is.EqualTo(true));
        for (byte nibble = 0; nibble < 16; nibble++)
        {
            Assert.That(branch.HasNibble(nibble), Is.EqualTo(false));
        }
    }


    private int GetTypeSize(Type type)
    {
        var dm = new DynamicMethod("$", typeof(int), Type.EmptyTypes);
        ILGenerator il = dm.GetILGenerator();
        il.Emit(OpCodes.Sizeof, type);
        il.Emit(OpCodes.Ret);

        return (int)(dm.Invoke(null, null) ?? -1);
    }
}
