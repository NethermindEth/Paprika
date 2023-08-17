using BenchmarkDotNet.Attributes;
using Paprika.Data;
using Paprika.Merkle;

namespace Paprika.Benchmarks;

[MemoryDiagnoser]
public class MerkleBenchmarks
{
    private static readonly Account Eoa = new(1_000_000, 100);

    [Benchmark]
    public byte EOA_Leaf()
    {
        Span<byte> key = stackalloc byte[1] { 1 };
        Node.Leaf.KeccakOrRlp(NibblePath.FromKey(key), in Eoa, out var result);

        // prevent not used result elimination
        return result.Span[0];
    }
}