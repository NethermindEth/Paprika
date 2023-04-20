using System.Runtime.CompilerServices;
using FluentAssertions;
using NUnit.Framework;
using Paprika.Pages;

namespace Paprika.Tests;

public class BitPool64Tests
{
    const int Max = 63;
    
    [Test]
    public void TryReserveBit()
    {
        BitPool64 bits = default;

        bits.TrySetLowestBit(Max, out var reserved);
        Assert.AreEqual(0, reserved);

        bits.TrySetLowestBit(Max, out reserved);
        Assert.AreEqual(1, reserved);

        bits.TrySetLowestBit(Max, out reserved);
        Assert.AreEqual(2, reserved);

        bits.ClearBit(1);

        bits.TrySetLowestBit(Max, out reserved);
        Assert.AreEqual(1, reserved);

        bits.TrySetLowestBit(Max, out reserved);
        Assert.AreEqual(3, reserved);
    }

    [Test]
    public void Full()
    {
        var allSet = ulong.MaxValue;
        BitPool64 bits = Unsafe.As<ulong, BitPool64>(ref allSet);
        bits.TrySetLowestBit(Max, out _).Should().BeFalse();
    }
}