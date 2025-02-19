using FluentAssertions;
using Paprika.Data;
using Paprika.Store;

namespace Paprika.Tests.Store;

public class BottomPageTests : BasePageTests
{
    private const uint BatchId = 1;

    [TestCase(true, TestName = "Empty key - first")]
    [TestCase(false, TestName = "Empty key - last")]
    public void Ordering(bool emptyFirst)
    {
        var batch = NewBatch(BatchId);
        var bottom = ((IBatchContext)batch).GetNewPage<BottomPage>(out _);

        var key = NibblePath.Empty;

        var key0 = NibblePath.FromKey([0x0A]);
        var key1 = NibblePath.FromKey([0x4A]);
        var key2 = NibblePath.FromKey([0x1A]);
        var key3 = NibblePath.FromKey([0x8A]);
        var key4 = NibblePath.FromKey([0xFA]);
        var key5 = NibblePath.FromKey([0xCA]);
        var key6 = NibblePath.FromKey([0x33]);
        var key7 = NibblePath.FromKey([0x34]);

        var v = new byte[1799];
        var v0 = new byte[1800];
        var v1 = new byte[1801];
        var v2 = new byte[1802];
        var v3 = new byte[1803];
        var v4 = new byte[1804];
        var v5 = new byte[1805];
        var v6 = new byte[1806];
        var v7 = new byte[1807];

        if (emptyFirst)
        {
            Set(key, v);
        }

        Set(key0, v0);
        Set(key1, v1);
        Set(key2, v2);
        Set(key3, v3);
        Set(key4, v4);
        Set(key5, v5);
        Set(key6, v6);
        Set(key7, v7);

        if (!emptyFirst)
        {
            Set(key, v);
        }

        Assert(key, v);
        Assert(key0, v0);
        Assert(key1, v1);
        Assert(key2, v2);
        Assert(key3, v3);
        Assert(key4, v4);
        Assert(key5, v5);
        Assert(key6, v6);
        Assert(key7, v7);

        return;

        void Set(in NibblePath path, byte[] value)
        {
            bottom.Set(path, value, batch);
        }

        void Assert(in NibblePath key, in ReadOnlySpan<byte> expected)
        {
            bottom.TryGet(batch, key, out var actual).Should().BeTrue();
            actual.SequenceEqual(expected).Should().BeTrue();
        }
    }
}