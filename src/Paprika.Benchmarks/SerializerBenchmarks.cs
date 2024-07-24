using System.Runtime.CompilerServices;
using BenchmarkDotNet.Attributes;
using Nethermind.Int256;
using Paprika.Crypto;
using Paprika.Data;

namespace Paprika.Benchmarks;

[MemoryDiagnoser]
// [DisassemblyDiagnoser]
public class SerializerBenchmarks
{
    private static ReadOnlySpan<byte> SmallSpan => new byte[] { 0, 1, 2, 3 };

    [Benchmark(OperationsPerInvoke = 4)]
    public ulong Read_uint256_full()
    {
        var span = Keccak.Zero.Span;

        Unsafe.SkipInit(out UInt256 value);

        ulong count = 0;

        Serializer.ReadFrom(span, out value);
        count += value.u1;

        Serializer.ReadFrom(span, out value);
        count += value.u1;

        Serializer.ReadFrom(span, out value);
        count += value.u1;

        Serializer.ReadFrom(span, out value);
        count += value.u1;

        return count;
    }

    [Benchmark(OperationsPerInvoke = 4)]
    public ulong Read_uint256_small()
    {
        Unsafe.SkipInit(out UInt256 value);

        ulong count = 0;

        Serializer.ReadFrom(SmallSpan, out value);
        count += value.u1;

        Serializer.ReadFrom(SmallSpan, out value);
        count += value.u1;

        Serializer.ReadFrom(SmallSpan, out value);
        count += value.u1;

        Serializer.ReadFrom(SmallSpan, out value);
        count += value.u1;

        return count;
    }
}
