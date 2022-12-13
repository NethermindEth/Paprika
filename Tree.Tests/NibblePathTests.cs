using System.Buffers.Binary;
using NUnit.Framework;

namespace Tree.Tests;

public class NibblePathTests
{
    [Test]
    public void Equal_From([Range(0, 15)] int from)
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

    [Test]
    public void FindFirstDifferent_DifferentLength([Values(1, 3, 4, 5, 7, 8)] int slice,
        [Values(true, false)] bool oddNibble)
    {
        const int length = 8;

        const ulong value = 0xFE_DC_BA_98_76_54_32_10;
        Span<byte> span = stackalloc byte[length];
        BinaryPrimitives.WriteUInt64LittleEndian(span, value);

        var oddity = oddNibble ? 1 : 0;
        var original = NibblePath.FromKey(span, oddity);
        var sliced = NibblePath.FromKey(span.Slice(0, slice), oddity);

        var found = original.FindFirstDifferentNibble(sliced);
        var expected = slice * 2 - oddity;
        Assert.AreEqual(expected, found);
    }

    [Test]
    public void SliceFrom([Values(0, 2, 4, 6, 8, 10, 12, 14)] int sliceFrom)
    {
        const int length = 8;
        const ulong value = 0xFE_DC_BA_98_76_54_32_10;
        Span<byte> span = stackalloc byte[length];
        BinaryPrimitives.WriteUInt64LittleEndian(span, value);

        var original = NibblePath.FromKey(span, 0);
        var sliced = original.SliceFrom(sliceFrom);
        var expected = NibblePath.FromKey(span.Slice(sliceFrom / 2), 0);

        Assert.IsTrue(expected.Equals(sliced));
    }

    [Test]
    public void ToString([Values(0, 1, 3, 4, 5, 7)] int from)
    {
        const int length = 4;
        const uint value = 0x76_54_32_10;
        Span<byte> span = stackalloc byte[length];
        BinaryPrimitives.WriteUInt32LittleEndian(span, value);

        var str = new string(value.ToString("X").Reverse().ToArray());

        Assert.AreEqual(str.Substring(from), NibblePath.FromKey(span, from).ToString());
    }

    [Test]
    public void NibbleAt([Values(0, 1, 2, 5)] int at, [Values(true, false)] bool odd)
    {
        const int length = 4;
        const uint value = 0x76_54_32_10;
        Span<byte> span = stackalloc byte[length];
        BinaryPrimitives.WriteUInt32LittleEndian(span, value);

        int oddity = odd ? 1 : 0;
        var path = NibblePath.FromKey(span, oddity);
        Assert.AreEqual(at + oddity, path.GetAt(at));
    }
}