using NUnit.Framework;

namespace Tree.Tests;

public class NibblesTests
{
    [Test]
    public void FindByteDifference_SamePaths([Values(1, 4, 7, 15, 24)] int length)
    {
        Span<byte> span = stackalloc byte[length];
        TestContext.CurrentContext.Random.NextBytes(span);

        ref var path = ref span[0];

        var diff = Nibbles.FindByteDifference(path, path, length);

        Assert.AreEqual(length, diff);
    }

    [Test]
    public void FindByteDifference_Within([Values(1, 8, 9, 16, 17)] int diffAt)
    {
        const int length = 20;
        Span<byte> span1 = stackalloc byte[length];
        Span<byte> span2 = stackalloc byte[length];
        TestContext.CurrentContext.Random.NextBytes(span1);

        span1.CopyTo(span2);

        // make them different
        span1[diffAt] = (byte)((span1[diffAt] + 1) & 0xFF);

        ref var path1 = ref span1[0];
        ref var path2 = ref span2[0];

        Assert.AreEqual(diffAt, Nibbles.FindByteDifference(path1, path2, length));
    }
}