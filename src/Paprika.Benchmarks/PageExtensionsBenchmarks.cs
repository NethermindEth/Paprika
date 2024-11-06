using BenchmarkDotNet.Attributes;
using Paprika.Data;
using Paprika.Store;

namespace Paprika.Benchmarks;

[DisassemblyDiagnoser]
public class PageExtensionsBenchmarks
{
    private readonly Page _a;
    private readonly Page _b;

    public unsafe PageExtensionsBenchmarks()
    {
        _a = new Page((byte*)Allocator.AllocAlignedPage());
        _b = new Page((byte*)Allocator.AllocAlignedPage());
    }

    [Benchmark]
    public void OrWith()
    {
        _a.OrWith(_b);
    }
}