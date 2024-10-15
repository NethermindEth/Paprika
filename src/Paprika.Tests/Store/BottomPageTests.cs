using FluentAssertions;
using Paprika.Data;
using Paprika.Store;

namespace Paprika.Tests.Store;

public class BottomPageTests : BasePageTests
{
    private const uint BatchId = 1;

    [Test]
    public void Sufficient_to_set()
    {
        var batch = NewBatch(BatchId);
        var bottom = ((IBatchContext)batch).GetNewPage<BottomPage>(out _);

        var key = NibblePath.Empty;
        var key0 = NibblePath.FromKey([0x0A]); // 0 is 0th nibble
        var key1 = NibblePath.FromKey([0x1A]); // 1 is 0th nibble
        var key8 = NibblePath.FromKey([0x8A]); // 8 is 0th nibble
        var key9 = NibblePath.FromKey([0x9A]); // 9 is 0th nibble

        var v0 = new byte[3002];
        var v1 = new byte[2999];
        var v8 = new byte[3003];
        var v9 = new byte[3006];
        var v = new byte[3001];

        bottom.Set(key0, v0, batch);
        bottom.Set(key1, v1, batch);
        bottom.Set(key8, v8, batch);
        bottom.Set(key9, v9, batch);
        bottom.Set(key, v, batch);

        Assert(key, v);
        Assert(key0, v0);
        Assert(key1, v1);
        Assert(key8, v8);
        Assert(key9, v9);
        return;

        void Assert(in NibblePath key, in ReadOnlySpan<byte> expected)
        {
            bottom.TryGet(batch, key, out var actual).Should().BeTrue();
            actual.SequenceEqual(expected).Should().BeTrue();
        }
    }
}