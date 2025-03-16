using FluentAssertions;
using Paprika.Store;

namespace Paprika.Tests.Store;

public class StorageFanOutTests
{
    private const uint Meg = 1024u * 1024u;

    [TestCase(0u)]
    [TestCase(1u)]
    [TestCase(1023u)]
    [TestCase(1024u)]
    [TestCase(1025u)]
    [TestCase(Meg - 1)]
    [TestCase(Meg)]
    [TestCase(Meg + 1)]
    [TestCase(Meg * 2 - 1)]
    [TestCase(Meg * 2)]
    [TestCase(Meg * 2 + 1)]
    [TestCase(Meg * 4 - 1)]
    [TestCase(Meg * 4)]
    [TestCase(Meg * 4 + 1)]
    [TestCase(Meg * 8 - 1)]
    [TestCase(Meg * 8)]
    [TestCase(Meg * 8 + 1)]
    [TestCase(Meg * 16 - 1)]
    [TestCase(Meg * 16)]
    [TestCase(Meg * 16 + 1)]
    [TestCase(Meg * 32 - 1)]
    [TestCase(Meg * 32)]
    [TestCase(Meg * 32 + 1)]
    [TestCase(Meg * 64 - 1)]
    [TestCase(Meg * 64)]
    [TestCase(Meg * 64 + 1)]
    public void AssertBoundaries(uint at)
    {
        (uint next0, int index0) = StorageFanOut.GetIndex(at, 0);
        index0.Should().BeLessThan(1024, $"L0 failed at: {at}");

        (uint next1, int index1) = StorageFanOut.GetIndex(next0, 1);
        index1.Should().BeLessThan(1024, $"L1 failed at: {at}");

        (byte bucket2, int index2) = StorageFanOut.Level2Page.GetIndex(next1);
        index2.Should().BeLessThan(256, $"L2 failed at: {at}");
        bucket2.Should().BeLessThan(16, $"L2 failed at: {at}");
    }
}