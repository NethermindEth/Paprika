using System.Buffers.Binary;
using FluentAssertions;
using NUnit.Framework;
using Paprika.Crypto;
using Paprika.Data;
using Paprika.Utils;

// ReSharper disable HeapView.BoxingAllocation

namespace Paprika.Tests.Data;

public class NibblePathTests
{
    [Test]
    public void Get_Singles()
    {
        // Expected values
        byte[] expectedSingles =
        [
            0x00, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77,
            0x88, 0x99, 0xAA, 0xBB, 0xCC, 0xDD, 0xEE, 0xFF
        ];

        // Get the actual values from the NibblePath.Singles property
        var actualSingles = NibblePath.SinglesForTests;

        // Check if the length is correct
        actualSingles.Length.Should().Be(expectedSingles.Length);

        // Check if each value is correct
        for (var i = 0; i < expectedSingles.Length; i++)
        {
            actualSingles[i].Should().Be(expectedSingles[i]);
        }
    }

    [Test]
    public void Get_Single([Values(0, 1)] int odd)
    {
        for (byte nibble = 0; nibble <= NibblePath.NibbleMask; nibble++)
        {
            var nibblePath = NibblePath.Single(nibble, odd);

            // Check the length
            nibblePath.Length.Should().Be(1);

            // Check the nibble value
            nibblePath[0].Should().Be(nibble);

            // Check the oddity
            bool isOdd = nibblePath.Oddity == 1;
            isOdd.Should().Be(odd == 1);
        }
    }

    [Test]
    public void FirstNibble_ShouldReturnCorrectValue()
    {
        var raw = new byte[] { 0x12, 0x34 };
        const byte evenFirst = 0x1;
        const byte oddFirst = 0x2;
        const byte single = 0xA;

        // Case 1: Even nibble path
        var evenNibblePath = NibblePath.FromKey(raw);
        evenNibblePath.FirstNibble.Should().Be(evenFirst);

        // Case 2: Odd nibble path
        var oddNibblePath = NibblePath.FromKey(raw, 1);
        oddNibblePath.FirstNibble.Should().Be(oddFirst);

        // Case 3: Single nibble path (even)
        var singleEvenNibblePath = NibblePath.Single(single, 0);
        singleEvenNibblePath.FirstNibble.Should().Be(single);

        // Case 4: Single nibble path (odd)
        var singleOddNibblePath = NibblePath.Single(single, 1);
        singleOddNibblePath.FirstNibble.Should().Be(single);
    }

    [TestCase(new byte[] { 0x12, 0x34, 0x56 }, 0, new byte[] { 0x12, 0x34, 0x56 })] // Even nibble path
    [TestCase(new byte[] { 0x12, 0x34, 0x56 }, 1, new byte[] { 0x12, 0x34, 0x56 })] // Odd nibble path
    [TestCase(new byte[] { 0x12, 0x34, 0x5 }, 1, new byte[] { 0x12, 0x34, 0x5 })]  // 5 length nibble path (odd)
    public void RawSpan_ShouldReturnCorrectSpan(byte[] raw, int nibbleOffset, byte[] expectedRaw)
    {
        var nibblePath = NibblePath.FromKey(raw, nibbleOffset);
        nibblePath.RawSpan.SequenceEqual(expectedRaw).Should().BeTrue();
    }


    [Test]
    public void RawSpan_ForSingleAndEmpty()
    {
        const byte singleRaw = 0xF;
        var expectedSingleRaw = new byte[] { 0xFF };

        var expectedEmptyRaw = Array.Empty<byte>();

        // Case: Single nibble path (even)
        var singleEvenNibblePath = NibblePath.Single(singleRaw, 0);
        singleEvenNibblePath.RawSpan.SequenceEqual(expectedSingleRaw).Should().BeTrue();

        // Case: Single nibble path (odd)
        var singleOddNibblePath = NibblePath.Single(singleRaw, 1);
        singleOddNibblePath.RawSpan.SequenceEqual(expectedSingleRaw).Should().BeTrue();

        // Case: Empty nibble path
        var emptyNibblePath = NibblePath.Empty;
        emptyNibblePath.RawSpan.SequenceEqual(expectedEmptyRaw).Should().BeTrue();
    }

    [Test]
    public void UnsafeAsKeccak_ShouldReturnCorrectKeccak()
    {
        string hexString = "380c98b03a3f72ee8aa540033b219c0d397dbe2523162db9dd07e6bbb015d50b";
        var nibblePath = NibblePath.Parse(hexString);

        Keccak expectedKeccak = new Keccak(Convert.FromHexString(hexString));
        Keccak actualKeccak = nibblePath.UnsafeAsKeccak;

        expectedKeccak.Should().Be(actualKeccak);

        Keccak expectedKeccak2 = Keccak.OfAnEmptyString;
        Keccak actualKeccak2 = NibblePath.Parse("c5d2460186f7233c927e7db2dcc703c0e500b653ca82273b7bfad8045d85a470").UnsafeAsKeccak;

        expectedKeccak2.Should().Be(actualKeccak2);
    }

    [Test]
    public void Equal_From([Range(0, 15)] int from)
    {
        const ulong value = 0xFE_DC_BA_98_76_54_32_10;
        Span<byte> span = stackalloc byte[8];
        BinaryPrimitives.WriteUInt64LittleEndian(span, value);

        var path = NibblePath.FromKey(span, from);

        Span<byte> destination = stackalloc byte[path.MaxByteLength];
        var leftover = path.WriteToWithLeftoverWithPreamble(destination);

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
        var written = path.WriteToWithPreamble(span);
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

        var writtenA = pathA.WriteToWithPreamble(stackalloc byte[pathA.MaxByteLength]);
        var writtenB = pathB.WriteToWithPreamble(stackalloc byte[pathB.MaxByteLength]);

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
    }

    [Test]
    public void Append_nibble_odd()
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
    public void Append_nibble_even()
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

    [Test]
    public void Append_include_hash()
    {
        var path1 = NibblePath.Parse("AB");
        var path2 = NibblePath.Parse("CDE");

        var appended = path1.Append(path2, 0x12, stackalloc byte[NibblePath.FullKeccakByteLength]);
        var expected = NibblePath.Parse("ABCDE12");
        appended.Equals(expected).Should().BeTrue();
    }

    [Test]
    public void Append_throw_exception()
    {
        // With hash
        Assert.Throws<ArgumentException>(() =>
        {
            var path1 = NibblePath.Single(0xA, 0);
            var path2 = NibblePath.Single(0xB, 0);
            path1.Append(path2, 0xCD, stackalloc byte[1]);
        });

        // Without hash
        Assert.Throws<ArgumentException>(() =>
        {
            var path1 = NibblePath.Single(0xA, 0);
            var path2 = NibblePath.Single(0xB, 0);
            path1.Append(path2, stackalloc byte[1]);
        });

        // AppendNibble
        // Without hash
        Assert.Throws<ArgumentException>(() =>
        {
            var path1 = NibblePath.Single(0xA, 0);
            var path2 = NibblePath.Single(0xB, 0);
            path1.AppendNibble(0xCD, stackalloc byte[1]);
        });
    }

    [Test]
    public void Append_emptyPath()
    {
        var path1 = NibblePath.Single(0xA, 0);
        var emptyPath = NibblePath.Empty;

        // Without hash
        var appended = path1.Append(emptyPath, stackalloc byte[NibblePath.FullKeccakByteLength]);
        var expected = NibblePath.Parse("A");

        appended.Equals(expected).Should().BeTrue();

        // With hash
        appended = path1.Append(emptyPath, 0xCD, stackalloc byte[NibblePath.FullKeccakByteLength]);
        expected = NibblePath.Parse("ACD");

        appended.Equals(expected).Should().BeTrue();
    }

    [Test]
    public void Append__oddity_alignment()
    {
        // Starting position of path1 and path2 is aligned
        var path1 = NibblePath.Parse("AB");
        var path2 = NibblePath.Parse("CD");

        var appended = path1.Append(path2, stackalloc byte[NibblePath.FullKeccakByteLength]);
        var expected = NibblePath.Parse("ABCD");

        appended.Equals(expected).Should().BeTrue();

        // Starting position of path1 and path2 is not aligned
        var path3 = NibblePath.Parse("A");
        var path4 = NibblePath.Parse("BCD");

        appended = path3.Append(path4, stackalloc byte[NibblePath.FullKeccakByteLength]);
        expected = NibblePath.Parse("ABCD");

        appended.Equals(expected).Should().BeTrue();

        // Case to increase coverage
        var path5 = NibblePath.Parse("ABC").SliceFrom(1);
        var path6 = NibblePath.Parse("DE").SliceFrom(1);

        appended = path5.Append(path6, stackalloc byte[NibblePath.FullKeccakByteLength]);
        expected = NibblePath.Parse("0BCE").SliceFrom(1); // To make odd

        appended.Equals(expected).Should().BeTrue();
    }

    [Test]
    public void Append_non_aligned_oddity_with_hash()
    {
        // Starting position of path1 and path2 is aligned
        var path1 = NibblePath.Parse("AB");
        var path2 = NibblePath.Parse("CD");

        var appended = path1.Append(path2, 0xEF, stackalloc byte[NibblePath.FullKeccakByteLength]);
        var expected = NibblePath.Parse("ABCDEF");

        appended.Equals(expected).Should().BeTrue();

        // Starting position of path1 and path2 is not aligned
        var path3 = NibblePath.Parse("A");
        var path4 = NibblePath.Parse("BCD");

        appended = path3.Append(path4, 0xEF, stackalloc byte[NibblePath.FullKeccakByteLength]);
        expected = NibblePath.Parse("ABCDEF");

        appended.Equals(expected).Should().BeTrue();

        var path5 = NibblePath.Parse("ABC").SliceFrom(1);
        var path6 = NibblePath.Parse("DE").SliceFrom(1);

        appended = path5.Append(path6, 0xAF, stackalloc byte[NibblePath.FullKeccakByteLength]);
        expected = NibblePath.Parse("0BCEAF").SliceFrom(1); // To make odd

        appended.Equals(expected).Should().BeTrue();
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

    [Test]
    public void HasOnlyZeroes()
    {
        var emptyNibblePath = NibblePath.Empty;
        emptyNibblePath.HasOnlyZeroes().Should().BeTrue();

        var span = new byte[] { 0x10 };

        var evenNibblePath = NibblePath.FromKey(span);
        evenNibblePath.HasOnlyZeroes().Should().BeFalse();

        var oddNibblePath = NibblePath.FromKey(span).SliceFrom(1);
        oddNibblePath.HasOnlyZeroes().Should().BeTrue();
    }

    [Test]
    public void UnsafeMakeOdd_ZeroLengthShouldThrow()
    {

        Assert.Throws<NullReferenceException>(() =>
        {
            var path = NibblePath.Empty;
            path.UnsafeMakeOdd();
        });

        Assert.Throws<NullReferenceException>(() =>
        {
            var path = NibblePath.Single((byte)0xA, 0).SliceFrom(1);
            path.UnsafeMakeOdd();
        });
    }

    [Test]
    public void UnsafeMakeOdd_SingleByteLength([Values(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15)] int nibble)
    {
        const string slicePrefix = "0";
        // Parse integer to hexadecimal
        var single = nibble.ToString("X");
        var path = NibblePath.Parse(single);
        path.UnsafeMakeOdd();

        var expectedString = string.Concat(slicePrefix, single);
        var expected = NibblePath.Parse(expectedString).SliceFrom(1);
        path.Equals(expected).Should().BeTrue();
        path.IsOdd.Should().BeTrue();
    }

    [Test]
    public void UnsafeMakeOdd_LengthTwoToFour([Values("ABCD", "ABC", "AB")] string pathString)
    {
        const string slicePrefix = "0";

        var path = NibblePath.Parse(pathString);
        path.UnsafeMakeOdd();

        path.IsOdd.Should().BeTrue();

        string expectedString = string.Concat(slicePrefix, pathString);
        var expected = NibblePath.Parse(expectedString).SliceFrom(1);
        path.Equals(expected).Should().BeTrue();
    }

    [Test]
    public void UnsafeMakeOdd_LargeLength()
    {
        const string slicePrefix = "0";
        const string pathString = "1234567890ABCDEF";
        var path = NibblePath.Parse(pathString);
        path.UnsafeMakeOdd();

        path.IsOdd.Should().BeTrue();

        var expected = NibblePath.Parse(string.Concat(slicePrefix, pathString)).SliceFrom(1);
        path.Equals(expected).Should().BeTrue();
    }

    [Test]
    public void TryReadFrom_ShouldReturnFalse_WhenPreambleDoesNotMatch()
    {
        var nibblePath = NibblePath.Parse("A1B2");
        var source = new byte[] { 0x00, 0xA1, 0xB2, 0xFF }; // Incorrect preamble
        var spanSource = new Span<byte>(source);

        var result = NibblePath.TryReadFrom(spanSource, in nibblePath, out var leftover);

        result.Should().BeFalse();
        leftover.ToArray().Should().BeEmpty();
    }

    [Test]
    public void TryReadFrom_ShouldReturnCorrectLeftover_WhenPreambleMatches()
    {
        var nibblePath = NibblePath.Parse("A1B2");
        var preamble = nibblePath.RawPreamble;

        var source = new byte[] { preamble, 0xA1, 0xB2, 0xCC, 0xDD }; // Correct preamble with extra data
        var spanSource = new Span<byte>(source);

        var expectedLeftover = new byte[] { 0xCC, 0xDD };

        var result = NibblePath.TryReadFrom(spanSource, in nibblePath, out var leftover);

        result.Should().BeTrue();
        leftover.ToArray().Should().Equal(expectedLeftover);
    }

    [Test]
    public void TryReadFrom_ShouldHandleEmptyPath()
    {
        var nibblePath = NibblePath.Empty;
        var preamble = nibblePath.RawPreamble;

        var storedData = new byte[] {0xCC, 0xDD};

        var source = storedData.Prepend(preamble).ToArray(); // [preamble, 0xCC, OxDD]
        var spanSource = new Span<byte>(source);

        var result = NibblePath.TryReadFrom(spanSource, in nibblePath, out var leftover);

        result.Should().BeTrue();
        leftover.ToArray().Should().Equal(storedData);
    }

    [Test]
    public void ReadFrom_ShouldHandleEmptyPath()
    {
        var source = new byte[] { 0x00 };
        var empty = Array.Empty<byte>();

        var spanSource = new Span<byte>(source);
        var leftover = NibblePath.ReadFrom(spanSource, out var nibblePath);

        nibblePath.IsEmpty.Should().BeTrue();
        leftover.ToArray().Should().Equal(empty);

        var roSpanSource = new ReadOnlySpan<byte>(source);
        var roLeftover = NibblePath.ReadFrom(roSpanSource, out nibblePath);

        nibblePath.IsEmpty.Should().BeTrue();
        roLeftover.ToArray().Should().Equal(empty);
    }

    [Test]
    public void ReadFrom_ShouldHandleSingleNibblePath()
    {
        var source = new byte[] { 0x02, 0xA0 }; // length = 1, odd = 0

        const int len = 1;
        const byte firstNibble = 0xA;
        var empty = Array.Empty<byte>();

        var spanSource = new Span<byte>(source);
        var leftover = NibblePath.ReadFrom(spanSource, out var nibblePath);

        nibblePath.Length.Should().Be(len);
        nibblePath[0].Should().Be(firstNibble);
        leftover.ToArray().Should().Equal(empty);

        var roSpanSource = new ReadOnlySpan<byte>(source);
        var roLeftover = NibblePath.ReadFrom(roSpanSource, out nibblePath);

        nibblePath.Length.Should().Be(len);
        nibblePath[0].Should().Be(firstNibble);
        roLeftover.ToArray().Should().Equal(empty);
    }

    [Test]
    public void ReadFrom_ShouldHandleOddNibblePath()
    {
        var source = new byte[] { 0x03, 0xAB, 0xC0 }; // length = 1, odd = 1
        var expected = new byte[] { 0xC0 };

        const int len = 1;
        const byte firstNibble = 0xB;

        var spanSource = new Span<byte>(source);
        var leftover = NibblePath.ReadFrom(spanSource, out var nibblePath);

        nibblePath.Length.Should().Be(len);
        nibblePath[0].Should().Be(firstNibble);
        nibblePath.IsOdd.Should().BeTrue();
        leftover.ToArray().Should().Equal(expected);

        var roSpanSource = new Span<byte>(source);
        var roLeftover = NibblePath.ReadFrom(roSpanSource, out nibblePath);

        nibblePath.Length.Should().Be(len);
        nibblePath[0].Should().Be(firstNibble);
        nibblePath.IsOdd.Should().BeTrue();
        roLeftover.ToArray().Should().Equal(expected);
    }

    [Test]
    public void ReadFrom_ShouldHandleEvenNibblePath()
    {
        var source = new byte[] { 0x04, 0xAB, 0xCD }; // length = 2, odd = 0
        var expected = new byte[] { 0xCD };

        const int len = 2;
        const int firstNibble = 0xA, secondNibble = 0xB;

        var spanSource = new Span<byte>(source);
        var leftover = NibblePath.ReadFrom(spanSource, out var nibblePath);

        nibblePath.Length.Should().Be(len);
        nibblePath[0].Should().Be(firstNibble);
        nibblePath[1].Should().Be(secondNibble);
        leftover.ToArray().Should().Equal(expected);

        var roSpanSource = new Span<byte>(source);
        var roLeftover = NibblePath.ReadFrom(roSpanSource, out nibblePath);

        nibblePath.Length.Should().Be(len);
        nibblePath[0].Should().Be(firstNibble);
        nibblePath[1].Should().Be(secondNibble);
        roLeftover.ToArray().Should().Equal(expected);
    }

    [Test]
    public void ReadFrom_ShouldHandleLongNibblePath()
    {
        var source = new byte[] { 0x0A, 0x12, 0x34, 0x56, 0x78, 0x9A, 0xBC, 0xDE, 0xF0 }; // length = 5, odd = 0
        var expected = new byte[] { 0x78, 0x9A, 0xBC, 0xDE, 0xF0 };

        const int len = 5;

        // Span<byte>
        var spanSource = new Span<byte>(source);
        var leftover = NibblePath.ReadFrom(spanSource, out var nibblePath);

        nibblePath.Length.Should().Be(len);

        for (var i = 1; i <= nibblePath.Length; i++)
        {
            nibblePath[i-1].ToString().Should().Be(i.ToString("X"));
        }

        leftover.ToArray().Should().Equal(expected);

        // ReadOnlySpan<byte>
        var roSpanSource = new  ReadOnlySpan<byte>(source);
        var roLeftover = NibblePath.ReadFrom(roSpanSource, out nibblePath);

        nibblePath.Length.Should().Be(len);

        for (var i = 1; i <= nibblePath.Length; i++)
        {
            nibblePath[i-1].ToString().Should().Be(i.ToString("X"));
        }

        roLeftover.ToArray().Should().Equal(expected);
    }

    [Test]
    public void ReadFrom_ShouldHandleOddNibblePathWithLeftover()
    {
        var source = new byte[] { 0x03, 0xAB, 0xCD, 0xEF }; // length = 1, odd = 1
        var expected = new byte[] { 0xCD, 0xEF };

        const int len = 1;
        const int oddFirstNibble = 0xB;

        var spanSource = new Span<byte>(source);
        var leftover = NibblePath.ReadFrom(spanSource, out var nibblePath);

        nibblePath.Length.Should().Be(len);
        nibblePath[0].Should().Be(oddFirstNibble);
        nibblePath.IsOdd.Should().BeTrue();
        leftover.ToArray().Should().Equal(expected);

        var roSpanSource = new Span<byte>(source);
        var roLeftover = NibblePath.ReadFrom(roSpanSource, out nibblePath);

        nibblePath.Length.Should().Be(len);
        nibblePath[0].Should().Be(oddFirstNibble);
        nibblePath.IsOdd.Should().BeTrue();
        roLeftover.ToArray().Should().Equal(expected);
    }
}
