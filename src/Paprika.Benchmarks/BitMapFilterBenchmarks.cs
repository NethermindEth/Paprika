using System.Runtime.InteropServices;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Diagnostics.dotMemory;
using Paprika.Data;
using Paprika.Store;

namespace Paprika.Benchmarks;

[DisassemblyDiagnoser(maxDepth: 2)]
[MemoryDiagnoser]
//[DotMemoryDiagnoser]

public class BitMapFilterBenchmarks
{
    private readonly Page[] _pages1A = AlignedAlloc(1);
    private readonly Page[] _pages1B = AlignedAlloc(1);

    private readonly Page[] _pages2A = AlignedAlloc(2);
    private readonly Page[] _pages2B = AlignedAlloc(2);

    private readonly Page[] _pages16A = AlignedAlloc(BitMapFilter.OfNSize128.Count);
    private readonly Page[] _pages16B = AlignedAlloc(BitMapFilter.OfNSize128.Count);

    private readonly BitMapFilter<BitMapFilter.OfN<BitMapFilter.OfNSize128>>[] _filters = Enumerable
        .Range(0, MaxFilterCount)
        .Select(i =>
            new BitMapFilter<BitMapFilter.OfN<BitMapFilter.OfNSize128>>(
                new BitMapFilter.OfN<BitMapFilter.OfNSize128>(AlignedAlloc(BitMapFilter.OfNSize128.Count))))
        .ToArray();

    private const int MaxFilterCount = 64;

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
        var a = new BitMapFilter<BitMapFilter.OfN<BitMapFilter.OfNSize128>>(new BitMapFilter.OfN<BitMapFilter.OfNSize128>(_pages16A));
        var b = new BitMapFilter<BitMapFilter.OfN<BitMapFilter.OfNSize128>>(new BitMapFilter.OfN<BitMapFilter.OfNSize128>(_pages16B));

        a.OrWith(b);
        a.OrWith(b);
        a.OrWith(b);
        a.OrWith(b);
    }

    [Benchmark]
    //[Arguments(16)]
    //[Arguments(32)]
    [Arguments(MaxFilterCount)]
    public void Or_BitMapFilter_OfN_128_Multiple(int count)
    {
        var a = new BitMapFilter<BitMapFilter.OfN<BitMapFilter.OfNSize128>>(new BitMapFilter.OfN<BitMapFilter.OfNSize128>(_pages16A));
        //var filters = _filters.AsSpan(0, count).ToArray();
        a.OrWith(_filters);
    }

    [Benchmark(OperationsPerInvoke = 4)]
    public int MayContainAny_BitMapFilter_OfN_128()
    {
        var a = new BitMapFilter<BitMapFilter.OfN<BitMapFilter.OfNSize128>>(new BitMapFilter.OfN<BitMapFilter.OfNSize128>(_pages16A));

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