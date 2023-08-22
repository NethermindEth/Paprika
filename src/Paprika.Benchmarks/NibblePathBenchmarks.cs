using BenchmarkDotNet.Attributes;
using Paprika.Crypto;
using Paprika.Data;

namespace Paprika.Benchmarks;

public class NibblePathBenchmarks
{
    [Params(true, false)]
    public bool FullKeccak { get; set; }

    [Params(0, 1, 2)]
    public int Slice { get; set; }

    [Benchmark(OperationsPerInvoke = 4)]
    public int Hash()
    {
        var span = FullKeccak ? Keccak.OfAnEmptyString.BytesAsSpan : stackalloc byte[3] { 0xFC, 234, 1 };
        var path = NibblePath.FromKey(
            span, Slice);

        return path.GetHashCode() ^
               path.GetHashCode() ^
               path.GetHashCode() ^
               path.GetHashCode();
    }
}