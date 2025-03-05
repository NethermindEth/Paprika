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

    [Benchmark(OperationsPerInvoke = 4)]
    [Arguments(false)]
    [Arguments(true)]
    public int Append_byte_aligned(bool oddEnd)
    {
        const int oddity = 1;

        var first = NibblePath.Single(0xA, oddity);
        Span<byte> bytes = [0x12, 0x34, 0x56, 0x78];

        var length = oddEnd ? bytes.Length - 1 : bytes.Length;
        var second = NibblePath.FromKey(bytes, 0, length);

        Span<byte> span = stackalloc byte[NibblePath.FullKeccakByteLength];

        return first.Append(second, span).Length +
               first.Append(second, span).Length +
               first.Append(second, span).Length +
               first.Append(second, span).Length;
    }

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
    [Arguments(0, 1)]
    [Arguments(1, 1)]
    [Arguments(0, 16)]
    [Arguments(1, 16)]
    [Arguments(0, 32)]
    [Arguments(1, 32)]
    [Arguments(0, 48)]
    [Arguments(1, 48)]
    [Arguments(0, NibblePath.KeccakNibbleCount)]
    public bool Equals(int slice, int length)
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