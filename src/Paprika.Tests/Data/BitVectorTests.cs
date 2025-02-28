using System.Collections.Specialized;
using FluentAssertions;
using Paprika.Data;

namespace Paprika.Tests.Data;

public class BitVectorTests
{
    [Test]
    public void Set_reset()
    {
        var v = new BitVector.Of1024();

        for (var i = 0; i < BitVector.Of1024.Count; i++)
        {
            v[i].Should().BeFalse();
            v[i] = true;
            v[i].Should().BeTrue();
            v[i] = false;
            v[i].Should().BeFalse();
        }
    }

    [TestCase(0)]
    [TestCase(1)]
    [TestCase(64)]
    [TestCase(65)]
    [TestCase(BitVector.Of1024.Count - 1)]
    [TestCase(BitVector.Of1024.Count)]
    public void First_not_set(int set)
    {
        var v = new BitVector.Of1024();

        for (int i = 0; i < set; i++)
        {
            v[i] = true;
        }

        v.FirstNotSet.Should().Be((ushort)set);
    }

    [TestCase(1, true)]
    [TestCase(BitVector.Of1024.Count, false)]
    public void Any_not_set(int set, bool anyNotSet)
    {
        var v = new BitVector.Of1024();

        for (int i = 0; i < set; i++)
        {
            v[i] = true;
        }

        v.HasEmptyBits.Should().Be(anyNotSet);
    }

    [TestCase(0)]
    [TestCase(63)]
    [TestCase(64)]
    [TestCase(127)]
    [TestCase(128)]
    [TestCase(BitVector.Of256.Count - 2)]
    [TestCase(BitVector.Of256.Count - 1)]
    public void HighestSmallerOrEqualThan(int set)
    {
        var v = new BitVector.Of256
        {
            [set] = true
        };

        for (var i = 0; i < BitVector.Of256.Count; i++)
        {
            var expected = i < set ? BitVector.NotFound : set;

            v.HighestSmallerOrEqualThan(i).Should().Be(expected);
        }
    }
}