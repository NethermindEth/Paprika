using BenchmarkDotNet.Attributes;
using Paprika.Store;

namespace Paprika.Benchmarks;

[DisassemblyDiagnoser]
public class DbAddressListBenchmarks
{
    private const int Count = DbAddressList.Of256.Count;

    [Benchmark(Baseline = true)]
    public uint Baseline()
    {
        Span<DbAddress> stack = stackalloc DbAddress[Count];

        for (var i = 0; i < Count; i++)
        {
            stack[i] = DbAddress.Page((uint)i);
        }

        uint sum = 0;
        for (var i = 0; i < Count; i++)
        {
            sum += stack[i].Raw;
        }

        return sum;
    }

    [Benchmark]
    public uint DbAddressList_Of256()
    {
        DbAddressList.Of256 stack = default;

        for (var i = 0; i < Count; i++)
        {
            stack[i] = DbAddress.Page((uint)i);
        }

        uint sum = 0;
        for (var i = 0; i < Count; i++)
        {
            sum += stack[i].Raw;
        }

        return sum;
    }
}