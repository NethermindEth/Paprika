using System.Runtime.InteropServices;
using BenchmarkDotNet.Attributes;
using Paprika.Data;
using Paprika.Store;

namespace Paprika.Benchmarks;

[DisassemblyDiagnoser]
[MemoryDiagnoser]
public class BitMapFilterBenchmarks
{
    private readonly Page[] _pages1A = AlignedAlloc(1);
    private readonly Page[] _pages1B = AlignedAlloc(1);

    private readonly Page[] _pages2A = AlignedAlloc(2);
    private readonly Page[] _pages2B = AlignedAlloc(2);

    private readonly Page[] _pages16A = AlignedAlloc(128);
    private readonly Page[] _pages16B = AlignedAlloc(128);

    [Benchmark(OperationsPerInvoke = 4)]
    public void Or_BitMapFilter_Of1()
    {
        var a = new BitMapFilter<BitMapFilter.Of1>(new BitMapFilter.Of1(_pages1A[0]));
        var b = new BitMapFilter<BitMapFilter.Of1>(new BitMapFilter.Of1(_pages1B[0]));

        a.OrWith(b);
        a.OrWith(b);
        a.OrWith(b);
        a.OrWith(b);
    }

    [Benchmark(OperationsPerInvoke = 4)]
    public void Or_BitMapFilter_Of2()
    {
        var a = new BitMapFilter<BitMapFilter.Of2>(new BitMapFilter.Of2(_pages2A[0], _pages2A[1]));
        var b = new BitMapFilter<BitMapFilter.Of2>(new BitMapFilter.Of2(_pages2B[0], _pages2B[1]));

        a.OrWith(b);
        a.OrWith(b);
        a.OrWith(b);
        a.OrWith(b);
    }

    [Benchmark(OperationsPerInvoke = 4)]
    public void Or_BitMapFilter_OfN_128()
    {
        var a = new BitMapFilter<BitMapFilter.OfN>(new BitMapFilter.OfN(_pages16A));
        var b = new BitMapFilter<BitMapFilter.OfN>(new BitMapFilter.OfN(_pages16B));

        a.OrWith(b);
        a.OrWith(b);
        a.OrWith(b);
        a.OrWith(b);
    }

    [Benchmark]
    [Arguments(16)]
    [Arguments(32)]
    [Arguments(64)]
    public void Or_BitMapFilter_OfN_128_Multiple(int count)
    {
        var a = new BitMapFilter<BitMapFilter.OfN>(new BitMapFilter.OfN(_pages16A));

        var filters = Enumerable.Range(0, count)
            .Select(i => new BitMapFilter<BitMapFilter.OfN>(new BitMapFilter.OfN(_pages16B)))
            .ToArray();

        a.OrWith(filters);
    }

    [Benchmark(OperationsPerInvoke = 4)]
    public int MayContainAny_BitMapFilter_OfN_128()
    {
        var a = new BitMapFilter<BitMapFilter.OfN>(new BitMapFilter.OfN(_pages16A));

        return (a.MayContainAny(13, 17) ? 1 : 0) +
               (a.MayContainAny(2342, 2345) ? 1 : 0) +
               (a.MayContainAny(3453453, 8789345) ? 1 : 0) +
               (a.MayContainAny(2346345, 432509) ? 1 : 0);
    }

    private static unsafe Page[] AlignedAlloc(int pageCount)
    {
        var pages = new Page[pageCount];

        for (int i = 0; i < pageCount; i++)
        {
            pages[i] = new Page((byte*)NativeMemory.AlignedAlloc(Page.PageSize, Page.PageSize));

            // make data more interesting
            pages[i].Span.Fill((byte)(1 << (i & 7)));
        }

        return pages;
    }
}