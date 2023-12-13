using System.Runtime.InteropServices;
using BenchmarkDotNet.Attributes;
using Paprika.Utils;

namespace Paprika.Benchmarks;

[DisassemblyDiagnoser(maxDepth: 2)]
public class XorBenchmarks
{
    private const int Size = 2000;
    private ulong[] _keys;
    private Xor8 _xor8;

    [GlobalSetup]
    public void Setup()
    {
        var random = new Random(13);

        _keys = new ulong[Size];
        random.NextBytes(MemoryMarshal.Cast<ulong, byte>(_keys));
        _xor8 = new Xor8(_keys);
    }

    [Benchmark]
    public bool MayContain()
    {
        var accumulator = false;
        for (var i = 0; i < _keys!.Length; i++)
        {
            accumulator ^= _xor8.MayContain(_keys[i]);
        }

        return accumulator;
    }
}