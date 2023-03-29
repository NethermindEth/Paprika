using NUnit.Framework;

namespace Paprika.Tests;

public class BitExtensionsTests
{
    [Test]
    public void TryReserveBit()
    {
        const int max = 31;
        uint bits = 0;

        BitExtensions.TrySetLowestBit(ref bits, max, out var reserved);
        Assert.AreEqual(0, reserved);

        BitExtensions.TrySetLowestBit(ref bits, max, out reserved);
        Assert.AreEqual(1, reserved);

        BitExtensions.TrySetLowestBit(ref bits, max, out reserved);
        Assert.AreEqual(2, reserved);

        BitExtensions.ClearBit(ref bits, 1);

        BitExtensions.TrySetLowestBit(ref bits, max, out reserved);
        Assert.AreEqual(1, reserved);

        BitExtensions.TrySetLowestBit(ref bits, max, out reserved);
        Assert.AreEqual(3, reserved);
    }
}