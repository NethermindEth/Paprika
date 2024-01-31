using System.Buffers.Binary;
using FluentAssertions;
using NUnit.Framework;
using Paprika.Data;

// ReSharper disable HeapView.BoxingAllocation

namespace Paprika.Tests.Data;

public class NibblePathTests
{
    [Test]
    public void Equal_From([Range(0, 15)] int from)
    {
        const ulong value = 0xFE_DC_BA_98_76_54_32_10;
        Span<byte> span = stackalloc byte[8];
        BinaryPrimitives.WriteUInt64LittleEndian(span, value);

        var path = NibblePath.FromKey(span, from);

        Span<byte> destination = stackalloc byte[path.MaxByteLength];
        var leftover = path.WriteToWithLeftover(destination);

        NibblePath.ReadFrom(destination, out var parsed);

        parsed.Equals(path).Should().BeTrue();
    }

    [Test]
    public void Get_at_even()
    {
        const byte first = 0xDC;
        const byte second = 0xBA;
        Span<byte> span = stackalloc byte[2] { first, second };

        var path = NibblePath.FromKey(span);

        Span<byte> expected = stackalloc byte[] { 0xD, 0xC, 0xB, 0xA };

        for (int i = 0; i < expected.Length; i++)
        {
            path.GetAt(i).Should().Be(expected[i]);
        }
    }

    [Test]
    public void Get_at_odd()
    {
        const byte first = 0xDC;
        const byte second = 0xBA;
        const byte third = 0x90;
        Span<byte> span = stackalloc byte[3] { first, second, third };

        var path = NibblePath.FromKey(span, 1);

        Span<byte> expected = stackalloc byte[] { 0xC, 0xB, 0xA, 0x9 };

        for (int i = 0; i < expected.Length; i++)
        {
            path.GetAt(i).Should().Be(expected[i]);
        }
    }

    private static void AssertDiffAt(in NibblePath path1, in NibblePath path2, int diffAt)
    {
        path1.FindFirstDifferentNibble(path2).Should().Be(diffAt);
        path2.FindFirstDifferentNibble(path1).Should().Be(diffAt);
    }

    [Test]
    public void FindFirstDifferent_equal_even_even()
    {
        var path1 = NibblePath.FromKey(stackalloc byte[] { 0xDC, 0xBA });
        var path2 = NibblePath.FromKey(stackalloc byte[] { 0xDC, 0xBA });

        AssertDiffAt(path1, path2, 4);
    }

    [Test]
    public void FindFirstDifferent_diff_even_even()
    {
        var path1 = NibblePath.FromKey(stackalloc byte[] { 0xD1, 0xBA });
        var path2 = NibblePath.FromKey(stackalloc byte[] { 0xDC, 0xBA });

        AssertDiffAt(path1, path2, 1);
    }

    [Test]
    public void FindFirstDifferent_equal_even_odd()
    {
        var path1 = NibblePath.FromKey(stackalloc byte[] { 0xDC, 0xBA });
        var path2 = NibblePath.FromKey(stackalloc byte[] { 0x0D, 0xCB, 0xA0 }, 1).SliceTo(path1.Length);

        AssertDiffAt(path1, path2, 4);
    }

    [Test]
    public void FindFirstDifferent_diff_even_odd()
    {
        var path1 = NibblePath.FromKey(stackalloc byte[] { 0xDC, 0x1A });
        var path2 = NibblePath.FromKey(stackalloc byte[] { 0x0D, 0xCB, 0xA0 }, 1).SliceTo(path1.Length);

        AssertDiffAt(path1, path2, 2);
    }

    [Test]
    public void FindFirstDifferent_diff_odd_odd()
    {
        var path1 = NibblePath.FromKey(stackalloc byte[] { 0x0D, 0xCB, 0xA0 }, 1).SliceTo(4);
        var path2 = NibblePath.FromKey(stackalloc byte[] { 0x0D, 0x1B, 0xA0 }, 1).SliceTo(4);

        AssertDiffAt(path1, path2, 1);
    }

    [Test]
    public void FindFirstDifferent_diff_at_0__odd_odd()
    {
        var path1 = NibblePath.FromKey(stackalloc byte[] { 0x0D, 0xCB, 0xA0 }, 1).SliceTo(4);
        var path2 = NibblePath.FromKey(stackalloc byte[] { 0x01, 0xCB, 0xA0 }, 1).SliceTo(4);

        AssertDiffAt(path1, path2, 0);
    }

    [Test]
    public void FindFirstDifferent_diff_at_last_long_even_even()
    {
        var path1 = NibblePath.FromKey(stackalloc byte[]
            { 0x12, 0x34, 0x56, 0x78, 0x9A, 0xBC, 0xDE, 0x24, 0x56, 0xAC });
        var path2 = NibblePath.FromKey(stackalloc byte[]
            { 0x12, 0x34, 0x56, 0x78, 0x9A, 0xBC, 0xDE, 0x24, 0x56, 0xA1 });

        AssertDiffAt(path1, path2, 19);
    }

    [Test]
    public void FindFirstDifferent_diff_at_1_long_even_even()
    {
        var path1 = NibblePath.FromKey(stackalloc byte[]
            { 0xA2, 0x34, 0x56, 0x78, 0x9A, 0xBC, 0xDE, 0x24, 0x56, 0xAC });
        var path2 = NibblePath.FromKey(stackalloc byte[]
            { 0xAD, 0x34, 0x56, 0x78, 0x9A, 0xBC, 0xDE, 0x24, 0x56, 0xAC });

        AssertDiffAt(path1, path2, 1);
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

        expected.Equals(sliced).Should().BeTrue();
    }

    [Test]
    public void ToString_even_even()
    {
        var path = NibblePath.FromKey(stackalloc byte[] { 0x12, 0x34, 0x56, 0x78 });
        path.ToString().Should().Be("12345678");
    }

    [Test]
    public void ToString_odd_odd()
    {
        var path = NibblePath.FromKey(stackalloc byte[] { 0x12, 0x34, 0x56, 0x78 }, 1);
        path.ToString().Should().Be("2345678");
    }

    [Test]
    public void ToString_odd_even()
    {
        var path = NibblePath.FromKey(stackalloc byte[] { 0x12, 0x34, 0x56, 0x78 }, 1).SliceTo(6);
        path.ToString().Should().Be("234567");
    }

    [Test]
    public void ToString_even_odd()
    {
        var path = NibblePath.FromKey(stackalloc byte[] { 0x12, 0x34, 0x56, 0x78 }).SliceTo(7);
        path.ToString().Should().Be("1234567");
    }

    [TestCase(0, false, 1)]
    [TestCase(3, false, 4)]
    [TestCase(0, true, 2)]
    [TestCase(3, true, 5)]
    public void NibbleAt(int at, bool odd, byte result)
    {
        var path = NibblePath.FromKey(stackalloc byte[] { 0x12, 0x34, 0x56 }, odd ? 1 : 0);
        path.GetAt(at).Should().Be(result);
    }

    [TestCase(0, 4)]
    [TestCase(0, 3)]
    [TestCase(1, 3)]
    [TestCase(1, 2)]
    public void Write_and_read(int from, int length)
    {
        const byte data = 253;
        var raw = new byte[] { 0x12, 0x34 };
        var path = NibblePath.FromKey(raw).SliceFrom(from).SliceTo(length);

        Span<byte> span = stackalloc byte[path.MaxByteLength + 1];
        var written = path.WriteTo(span);
        span[written.Length] = data;

        var left = NibblePath.ReadFrom(span.Slice(0, written.Length + 1), out var actual);

        // assert
        actual.ToString().Should().Be(path.ToString());

        left.Length.Should().Be(1);
        left[0].Should().Be(data);
    }

    [Test]
    public void Writing_path_with_last_nibble_skipped_should_not_care_about_the_rest()
    {
        const int length = 3;

        var pathA = NibblePath.FromKey(new byte[] { 0x12, 0x3A }).SliceTo(length);
        var pathB = NibblePath.FromKey(new byte[] { 0x12, 0x3B }).SliceTo(length);

        var writtenA = pathA.WriteTo(stackalloc byte[pathA.MaxByteLength]);
        var writtenB = pathB.WriteTo(stackalloc byte[pathB.MaxByteLength]);

        writtenA.SequenceEqual(writtenB).Should().BeTrue();
    }

    [TestCase(0, 4)]
    [TestCase(0, 3)]
    [TestCase(1, 3)]
    [TestCase(1, 2)]
    public void AppendNibble(int from, int length)
    {
        const byte first = 0xDC;
        const byte second = 0xBA;
        const byte nibble = 0xF;

        Span<byte> span = stackalloc byte[2] { first, second };

        var path = NibblePath.FromKey(span, from).SliceTo(length);
        var appended = path.AppendNibble(nibble, stackalloc byte[path.MaxByteLength + 1]);

        appended.Length.Should().Be((byte)(length + 1));
        appended.FindFirstDifferentNibble(path).Should().Be(length);
        appended.GetAt(length).Should().Be(nibble);

        Console.WriteLine(path.ToString());
    }

    [Test]
    public void PrependNibble_odd()
    {
        const byte first = 0xDC;
        const byte second = 0xBA;
        const byte nibble = 0xE;

        Span<byte> span = stackalloc byte[2] { first, second };
        var path = NibblePath.FromKey(span, 1);

        var firstNibblePath =
            NibblePath
                .FromKey(stackalloc byte[1] { nibble << NibblePath.NibbleShift })
                .SliceTo(1);

        var appended = firstNibblePath.Append(path, stackalloc byte[NibblePath.FullKeccakByteLength]);
        Span<byte> expected = stackalloc byte[2] { (nibble << NibblePath.NibbleShift) | (first & 0x0F), second };

        NibblePath.FromKey(expected).Equals(appended);
    }

    [Test]
    public void PrependNibble_even()
    {
        const byte first = 0xDC;
        const byte nibble = 0xE;

        Span<byte> span = stackalloc byte[1] { first };
        var path = NibblePath.FromKey(span, 1);

        var firstNibblePath =
            NibblePath
                .FromKey(stackalloc byte[1] { nibble << NibblePath.NibbleShift })
                .SliceTo(1);

        var appended = firstNibblePath.Append(path, stackalloc byte[NibblePath.FullKeccakByteLength]);
        Span<byte> expected = stackalloc byte[2] { nibble, first };

        NibblePath.FromKey(expected).SliceFrom(1).Equals(appended);
    }

    [TestCaseSource(nameof(GetRawNibbles))]
    public void Raw_nibbles(byte[] nibbles)
    {
        var path = NibblePath.FromRawNibbles(nibbles, stackalloc byte[(nibbles.Length + 1) / 2]);

        path.Length.Should().Be((byte)nibbles.Length);
        for (var i = 0; i < nibbles.Length; i++)
        {
            path[i].Should().Be(nibbles[i]);
        }
    }

    public static IEnumerable<TestCaseData> GetRawNibbles()
    {
        yield return new TestCaseData(new byte[] { 1, 2, 3 }).SetName("Short - odd");
        yield return new TestCaseData(new byte[] { 1, 2, 3, 4 }).SetName("Short - even");

        var @long = new byte[]
        {
            1, 2, 3, 4, 5, 6, 7, 8, 9, 0xA, 0xB, 0xC, 0xD, 0xE,
            1, 2, 3, 4, 5, 6, 7, 8, 9, 0xA, 0xB, 0xC, 0xD, 0xE,
            1, 2, 3, 4, 5, 6, 7, 8, 9, 0xA, 0xB, 0xC, 0xD, 0xE,
            1, 2, 3, 4, 5, 6, 7, 8, 9, 0xA, 0xB, 0xC, 0xD, 0xE,
        };

        yield return new TestCaseData(@long.AsSpan()[..^1].ToArray()).SetName("Long - odd");
        yield return new TestCaseData(@long).SetName("Long - even");
    }

    [Parallelizable(ParallelScope.None)]
    [TestCase(0, 0, TestName = "Empty")]
    [TestCase(0, 1, TestName = "Single, start from 0")]
    [TestCase(1, 1, TestName = "Single, start from 1")]
    [TestCase(1, 4, TestName = "Odd 1")]
    [TestCase(1, 7, TestName = "Odd 2")]
    [TestCase(1, 9, TestName = "Odd 3")]
    [TestCase(1, 11, TestName = "Odd 4")]
    [TestCase(1, 19, TestName = "Odd 5")]
    [TestCase(0, 4, TestName = "Even 1")]
    [TestCase(0, 7, TestName = "Even 2")]
    [TestCase(0, 9, TestName = "Even 3")]
    [TestCase(0, 11, TestName = "Even 4")]
    [TestCase(0, 20, TestName = "Even 5")]
    public void GetHashCode(int from, int length)
    {
        Span<byte> span = stackalloc byte[10] { 0x12, 0x34, 0x56, 0x78, 0x9A, 0xBC, 0xDE, 0xF0, 0x73, 0x64 };

        var path = NibblePath.FromKey(span).SliceFrom(from).SliceTo(length);
        var hash = path.GetHashCode();

        _hashes.Add(hash).Should().BeTrue();
    }

    private HashSet<int> _hashes = new();
}