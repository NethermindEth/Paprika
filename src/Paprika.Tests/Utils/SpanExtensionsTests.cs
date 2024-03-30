using FluentAssertions;
using Paprika.Utils;

namespace Paprika.Tests.Utils;

public class SpanExtensionsTests
{
    [Test]
    public void Batch_empty()
    {
        var e = Span<int>.Empty.BatchConsecutive();

        End(e);
    }

    [Test]
    public void Batch_single_consecutive()
    {
        Span<int> span = [1, 2, 3];

        var e = span.BatchConsecutive();

        MoveNext(ref e, new Range(0, span.Length));

        End(e);
    }

    [Test]
    public void Batch_single_consecutive_with_max()
    {
        Span<int> span = [1, 2, 3, 4];

        const int max = 2;

        var e = span.BatchConsecutive(max);

        MoveNext(ref e, new Range(0, max));
        MoveNext(ref e, new Range(2, max));

        End(e);
    }

    [Test]
    public void Batch_splits()
    {
        Span<int> span = [1, 3, 4, 5, 6];

        const int at = 1;

        var e = span.BatchConsecutive();

        MoveNext(ref e, new Range(0, at));
        MoveNext(ref e, new Range(at, span.Length - at));

        End(e);
    }

    [Test]
    public void Batch_splits_with_max()
    {
        Span<int> span = [1, 3, 4, 5, 6, 7];

        const int max = 2;

        var e = span.BatchConsecutive(max);

        MoveNext(ref e, new Range(0, 1));
        MoveNext(ref e, new Range(1, max));
        MoveNext(ref e, new Range(3, max));
        MoveNext(ref e, new Range(5, 1));

        End(e);
    }

    private static void MoveNext(ref SpanExtensions.RangeEnumerator<int> e, Range range)
    {
        e.MoveNext().Should().BeTrue();
        e.Current.Should().Be(range);
    }

    private static void End(SpanExtensions.RangeEnumerator<int> e)
    {
        e.MoveNext().Should().BeFalse();
    }
}