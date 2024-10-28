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
            filter.MayContain(i).Should().BeFalse();
            filter.Add(i);
            filter.MayContain(i).Should().BeTrue();
        }

        filter.Return(_pool);
    }

    [Test]
    public void Or_with()
    {
        var filter1 = Build(_pool);
        var filter2 = Build(_pool);

        const ulong v1 = 1;
        const ulong v2 = 4213798855897314219;

        filter1.Add(v1);
        filter2.Add(v2);

        filter1.OrWith(filter2);

        filter1.MayContain(v1).Should().BeTrue();
        filter1.MayContain(v2).Should().BeTrue();
        filter1.MayContainAny(v1, v2).Should().BeTrue();

        filter1.Return(_pool);
        filter2.Return(_pool);
    }

    [Test]
    public void Atomic_non_colliding_sets()
    {
        var filter = Build(_pool);

        var count = filter.BucketCount;

        Parallel.For(0, count, i =>
        {
            var hash = (uint)i;
            filter.MayContainVolatile(hash).Should().BeFalse();
            filter.AddAtomic(hash).Should().BeTrue();
            filter.MayContainVolatile(hash).Should().BeTrue();
            filter.AddAtomic(hash).Should().BeFalse();
        });

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



