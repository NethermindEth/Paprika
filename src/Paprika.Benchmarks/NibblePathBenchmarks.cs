using BenchmarkDotNet.Attributes;
using Paprika.Crypto;
using Paprika.Data;

namespace Paprika.Benchmarks;

[DisassemblyDiagnoser]
public class NibblePathBenchmarks
{
    private static readonly Keccak A1 = Keccak.OfAnEmptyString;
    private static readonly Keccak A2 = Keccak.OfAnEmptyString;
    private static readonly Keccak B = Keccak.Zero;

    [Benchmark(OperationsPerInvoke = 2)]
    public bool Equals_not_equal_sliced()
    {
        // hammer odd and not
        var a = NibblePath.FromKey(A1);
        var b = NibblePath.FromKey(B);

        var a1 = a.SliceFrom(1);
        var b1 = b.SliceFrom(1);

        return a.Equals(b) ^ a1.Equals(b1);
    }

    [Benchmark(OperationsPerInvoke = 2)]
    public bool Equals_equal_sliced()
    {
        // hammer odd and not
        var a = NibblePath.FromKey(A1);
        var b = NibblePath.FromKey(A2);

        var a1 = a.SliceFrom(1);
        var b1 = b.SliceFrom(1);

        return a.Equals(b) ^ a1.Equals(b1);
    }

    [Benchmark(OperationsPerInvoke = 4)]
    [Arguments(true, 0)]
    [Arguments(true, 1)]
    [Arguments(true, 2)]
    [Arguments(false, 0)]
    [Arguments(false, 1)]
    [Arguments(false, 2)]
    public int Hash(bool fullKeccak, int slice)
    {
        var span = fullKeccak ? Keccak.OfAnEmptyString.BytesAsSpan : stackalloc byte[3] { 0xFC, 234, 1 };
        var path = NibblePath.FromKey(
            span, slice);

        return path.GetHashCode() ^
               path.GetHashCode() ^
               path.GetHashCode() ^
               path.GetHashCode();
    }
}