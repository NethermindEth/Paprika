using BenchmarkDotNet.Attributes;
using Paprika.Chain;

namespace Paprika.Benchmarks;

[MemoryDiagnoser]
public class PooledSpanDictionaryBenchmarks
{
    private readonly BufferPool _pool = new(1, false);

    [Benchmark]
    public int Read_write_small()
    {
        using var dict = new PooledSpanDictionary(_pool, false);

        Span<byte> key = stackalloc byte[2];

        for (byte i = 0; i < 255; i++)
        {
            key[0] = i;
            dict.Set(key, i, key, 1);
        }

        var count = 0;
        for (byte i = 0; i < 255; i++)
        {
            key[0] = i;
            dict.TryGet(key, (ulong)i, out var result);
            count += result[0];
        }

        return count;
    }
}
