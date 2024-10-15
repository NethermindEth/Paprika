using FluentAssertions;
using Paprika.Data;
using Paprika.Store;

namespace Paprika.Tests.Store;

public class BottomPageTests : BasePageTests
{
    private const uint BatchId = 1;

    [Test]
    public void Spinning_through_same_keys_should_use_limited_number_of_pages()
    {
        var batch = NewBatch(BatchId);
        var bottom = ((IBatchContext)batch).GetNewPage<BottomPage>(out _);

        var key = NibblePath.Empty;
        var key0 = NibblePath.FromKey([0x0A]); // 0 is 0th nibble
        var key8 = NibblePath.FromKey([0x8A]); // 8 is 0th nibble
        var v0 = new byte[3002];
        var v8 = new byte[3003];
        var v = new byte[3001];

        bottom.Set(key0, v0, batch);
        bottom.Set(key8, v8, batch);
        bottom.Set(key, v, batch);

        Assert(key, v);
        Assert(key0, v0);
        Assert(key8, v8);
        return;

        void Assert(in NibblePath key, in ReadOnlySpan<byte> expected)
        {
            bottom.TryGet(batch, key, out var actual).Should().BeTrue();
            actual.SequenceEqual(expected).Should().BeTrue();
        }
    }
}