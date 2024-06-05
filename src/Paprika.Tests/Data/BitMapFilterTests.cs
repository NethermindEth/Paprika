using FluentAssertions;
using Paprika.Chain;
using Paprika.Data;

namespace Paprika.Tests.Data;

public abstract class BitMapFilterTests<TAccessor> : IDisposable
    where TAccessor : struct, BitMapFilter.IAccessor<TAccessor>
{
    private readonly BufferPool _pool = new(32);

    protected abstract BitMapFilter<TAccessor> Build(BufferPool pool);

    [Test]
    public void Non_colliding_set()
    {
        var filter = Build(_pool);

        var count = (ulong)filter.BucketCount;

        for (ulong i = 0; i < count; i++)
        {
            filter[i].Should().BeFalse();
            filter[i] = true;
            filter[i].Should().BeTrue();
        }

        filter.Return(_pool);
    }

    public void Dispose() => _pool.Dispose();
}

[TestFixture]
public class BitMapFilterTestsOf1 : BitMapFilterTests<BitMapFilter.Of1>
{
    protected override BitMapFilter<BitMapFilter.Of1> Build(BufferPool pool) => BitMapFilter.CreateOf1(pool);
}

[TestFixture]
public class BitMapFilterTestsOf2 : BitMapFilterTests<BitMapFilter.Of2>
{
    protected override BitMapFilter<BitMapFilter.Of2> Build(BufferPool pool) => BitMapFilter.CreateOf2(pool);
}

[TestFixture]
public class BitMapFilterTestsOf4 : BitMapFilterTests<BitMapFilter.OfN>
{
    protected override BitMapFilter<BitMapFilter.OfN> Build(BufferPool pool) => BitMapFilter.CreateOfN(pool, 4);
}



