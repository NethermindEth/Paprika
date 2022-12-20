using System.Buffers.Binary;
using System.Runtime.InteropServices;
using NUnit.Framework;

namespace Tree.Tests;

public partial class PaprikaTreeTests
{
    [Test]
    public void RLP_Leaf()
    {
        var key = NibblePath.FromKey(stackalloc byte[] { 0x12, 0x34 });
        Span<byte> value = stackalloc byte[] { 3, 5, 7, 11 };
        var expected = new byte[] { 201, 131, 32, 18, 52, 132, 3, 5, 7, 11 };
        
        AssertLeaf(expected, key, value);
    }

    private static void AssertLeaf(byte[] expected, in NibblePath path, in ReadOnlySpan<byte> value)
    {
        Span<byte> destination = stackalloc byte[32];
        var encoded = PaprikaTree.EncodeLeaf(path, value, destination);
        if (encoded == PaprikaTree.HasKeccak)
        {
            // keccak
            CollectionAssert.AreEqual(expected, destination.ToArray());
        }
        else
        {
            // rlp
            var length = destination[0];
            var rlp = destination.Slice(1, length);
            CollectionAssert.AreEqual(expected, rlp.ToArray());
        }
    }
}