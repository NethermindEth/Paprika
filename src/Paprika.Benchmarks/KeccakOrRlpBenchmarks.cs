using BenchmarkDotNet.Attributes;
using Paprika.Crypto;
using Paprika.RLP;

namespace Paprika.Benchmarks;

[MemoryDiagnoser]
// [DisassemblyDiagnoser]
public class KeccakOrRlpBenchmarks
{
    private static ReadOnlySpan<byte> RlpSpan => new byte[] { 0, 1, 2, 3, 4, 5, 7, 8 };

    [Benchmark(OperationsPerInvoke = 4)]
    public int From_span_rlp()
    {
        int length = 0;
        KeccakOrRlp keccak;

        KeccakOrRlp.FromSpan(RlpSpan, out keccak);
        length += keccak.Length;
        KeccakOrRlp.FromSpan(RlpSpan, out keccak);
        length += keccak.Length;
        KeccakOrRlp.FromSpan(RlpSpan, out keccak);
        length += keccak.Length;
        KeccakOrRlp.FromSpan(RlpSpan, out keccak);
        length += keccak.Length;

        return length;
    }

    [Benchmark(OperationsPerInvoke = 4)]
    public int From_span_keccak()
    {
        var span = Keccak.EmptyTreeHash.Span;

        int length = 0;
        KeccakOrRlp keccak;

        KeccakOrRlp.FromSpan(span, out keccak);
        length += keccak.Length;
        KeccakOrRlp.FromSpan(span, out keccak);
        length += keccak.Length;
        KeccakOrRlp.FromSpan(span, out keccak);
        length += keccak.Length;
        KeccakOrRlp.FromSpan(span, out keccak);
        length += keccak.Length;

        return length;
    }
}
