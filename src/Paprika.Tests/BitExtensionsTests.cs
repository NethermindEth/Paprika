using NUnit.Framework;
using Paprika.Pages;

namespace Paprika.Tests;

public class BitPool32Tests
{
    [Test]
    public void TryReserveBit()
    {
        const int max = 31;
        BitPool32 bits = default;

        bits.TrySetLowestBit(max, out var reserved);
        Assert.AreEqual(0, reserved);

        bits.TrySetLowestBit(max, out reserved);
        Assert.AreEqual(1, reserved);

        bits.TrySetLowestBit(max, out reserved);
        Assert.AreEqual(2, reserved);

        bits.ClearBit(1);

        bits.TrySetLowestBit(max, out reserved);
        Assert.AreEqual(1, reserved);

        bits.TrySetLowestBit(max, out reserved);
        Assert.AreEqual(3, reserved);
    }
}