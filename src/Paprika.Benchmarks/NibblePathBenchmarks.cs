using BenchmarkDotNet.Attributes;
using Paprika.Crypto;
using Paprika.Data;

namespace Paprika.Benchmarks;

public class NibblePathBenchmarks
{
    [Params(0, 1, 2, 3)]
    public int Slice { get; set; }

    [Benchmark(OperationsPerInvoke = 4)]
    public int Hash_short()
    {
        var path = NibblePath.FromKey(stackalloc byte[3] { 0xFC, 234, 1 }, Slice);

        return path.GetHashCode() ^
               path.GetHashCode() ^
               path.GetHashCode() ^
               path.GetHashCode();
    }

    [Benchmark(OperationsPerInvoke = 4)]
    public int Hash_Keccak()
    {
        var path = NibblePath.FromKey(Keccak.OfAnEmptyString, Slice);

        return path.GetHashCode() ^
               path.GetHashCode() ^
               path.GetHashCode() ^
               path.GetHashCode();
    }
}