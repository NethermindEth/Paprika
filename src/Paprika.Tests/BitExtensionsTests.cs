using NUnit.Framework;

namespace Paprika.Tests;

public class BitExtensionsTests
{
    [Test]
    public void TryReserveBit()
    {
        const int max = 31;
        uint bits = 0;

        BitExtensions.TryReserveBit(ref bits, max, out var reserved);
        Assert.AreEqual(0, reserved);

        BitExtensions.TryReserveBit(ref bits, max, out reserved);
        Assert.AreEqual(1, reserved);

        BitExtensions.TryReserveBit(ref bits, max, out reserved);
        Assert.AreEqual(2, reserved);
    }
}