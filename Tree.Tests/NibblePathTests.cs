using System.Buffers.Binary;
using NUnit.Framework;

namespace Tree.Tests;

public class NibblePathTests
{
    [Test]
    public void Equal_From([Range(0, 15)]int from)
    {
        const ulong value = 0xFE_DC_BA_98_76_54_32_10;
        Span<byte> span = stackalloc byte[8];
        BinaryPrimitives.WriteUInt64LittleEndian(span, value);

        var path = NibblePath.FromKey(span, from);
        
        Span<byte> destination = stackalloc byte[path.MaxLength];
        var leftover = path.WriteTo(destination);

        NibblePath.ReadFrom(destination, out var parsed);
        
        Assert.IsTrue(parsed.Equals(path));
    }
}