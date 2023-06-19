using NUnit.Framework;
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
    }
}
