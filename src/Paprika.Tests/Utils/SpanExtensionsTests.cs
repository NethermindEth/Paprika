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
        const int start0 = 1;
        Span<int> span = [start0, 2, 3];

        var e = span.BatchConsecutive();

        MoveNext(ref e, new(start0, span.Length));

        End(e);
    }

    [Test]
    public void Batch_single_consecutive_with_max()
    {
        const int start0 = 1;
        const int start1 = 3;
        Span<int> span = [start0, 2, start1, 4];

        const int max = 2;

        var e = span.BatchConsecutive(max);

        MoveNext(ref e, new(start0, max));
        MoveNext(ref e, new(start1, max));

        End(e);
    }

    [Test]
    public void Batch_splits()
    {
        const int start0 = 1;
        const int start1 = 3;
        Span<int> span = [start0, start1, 4, 5, 6];

        const int at = 1;

        var e = span.BatchConsecutive();

        MoveNext(ref e, new(start0, at));
        MoveNext(ref e, new(start1, span.Length - at));

        End(e);
    }

    [Test]
    public void Batch_splits_with_max()
    {
        const int start0 = 1;
        const int start1 = 3;
        const int start2 = 5;
        const int start3 = 7;

        Span<int> span = [start0, start1, 4, start2, 6, start3];

        const int max = 2;

        var e = span.BatchConsecutive(max);

        MoveNext(ref e, new(start0, 1));
        MoveNext(ref e, new(start1, max));
        MoveNext(ref e, new(start2, max));
        MoveNext(ref e, new(start3, 1));

        End(e);
    }

    private static void MoveNext(ref SpanExtensions.RangeEnumerator<int> e, (int Start, int Length) range)
    {
        e.MoveNext().Should().BeTrue();
        e.Current.Should().Be(range);
    }

    private static void End(SpanExtensions.RangeEnumerator<int> e)
    {
        e.MoveNext().Should().BeFalse();
    }
}