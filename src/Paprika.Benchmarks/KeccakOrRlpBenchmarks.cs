using BenchmarkDotNet.Attributes;
using Paprika.Crypto;
using Paprika.RLP;

namespace Paprika.Benchmarks;

[MemoryDiagnoser]
[DisassemblyDiagnoser]
public class KeccakOrRlpBenchmarks
{
    private static ReadOnlySpan<byte> RlpSpan => new byte[] { 0, 1, 2, 3, 4, 5, 7, 8 };

    [Benchmark(OperationsPerInvoke = 4)]
    public int From_span_rlp()
    {
        return KeccakOrRlp.FromSpan(RlpSpan).Span.Length +
            KeccakOrRlp.FromSpan(RlpSpan).Span.Length +
            KeccakOrRlp.FromSpan(RlpSpan).Span.Length +
            KeccakOrRlp.FromSpan(RlpSpan).Span.Length;
    }

    [Benchmark(OperationsPerInvoke = 4)]
    public int From_span_keccak()
    {
        var span = Keccak.EmptyTreeHash.Span;

        return KeccakOrRlp.FromSpan(span).Span.Length +
               KeccakOrRlp.FromSpan(span).Span.Length +
               KeccakOrRlp.FromSpan(span).Span.Length +
               KeccakOrRlp.FromSpan(span).Span.Length;
    }
}