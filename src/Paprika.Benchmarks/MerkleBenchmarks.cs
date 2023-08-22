using BenchmarkDotNet.Attributes;
using Paprika.Crypto;
using Paprika.Data;
using Paprika.Merkle;

namespace Paprika.Benchmarks;

public class MerkleBenchmarks
{
    private static readonly Keccak SomeKeccak = Keccak.Compute(new byte[] { 1 });

    private static readonly Account Eoa = new(1_000_000, 100);
    private static readonly Account Contract = new(1_000_000, 100, SomeKeccak, SomeKeccak);

    [Benchmark]
    public byte EOA_Leaf()
    {
        Span<byte> key = stackalloc byte[1] { 1 };
        Node.Leaf.KeccakOrRlp(NibblePath.FromKey(key), in Eoa, out var result);

        // prevent not used result elimination
        return result.Span[0];
    }

    [Benchmark]
    public byte Contract_Leaf()
    {
        Span<byte> key = stackalloc byte[1] { 1 };
        Node.Leaf.KeccakOrRlp(NibblePath.FromKey(key), in Contract, out var result);

        // prevent not used result elimination
        return result.Span[0];
    }

    [Benchmark]
    public byte Storage_long_path()
    {
        Span<byte> value = stackalloc byte[1] { 1 };
        Node.Leaf.KeccakOrRlp(NibblePath.FromKey(SomeKeccak), value, out var result);

        // prevent not used result elimination
        return result.Span[0];
    }

    [Benchmark]
    public byte Storage_short_path()
    {
        Span<byte> key = stackalloc byte[1] { 1 };
        Span<byte> value = stackalloc byte[1] { 3 };
        Node.Leaf.KeccakOrRlp(NibblePath.FromKey(key), value, out var result);

        // prevent not used result elimination
        return result.Span[0];
    }
}