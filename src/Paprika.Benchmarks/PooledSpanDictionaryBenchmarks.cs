using System.Buffers.Binary;
using BenchmarkDotNet.Attributes;
using Paprika.Chain;
using Paprika.Crypto;
using Paprika.Data;

namespace Paprika.Benchmarks;

[DisassemblyDiagnoser]
[MemoryDiagnoser]
public class PooledSpanDictionaryBenchmarks
{
    private readonly BufferPool _pool = new(1, false);

    private readonly PooledSpanDictionary _bigDictionary;
    private const int ReadCount = 32_000;

    public PooledSpanDictionaryBenchmarks()
    {
        _bigDictionary = new PooledSpanDictionary(_pool, false);

        Span<byte> span = stackalloc byte[128];

        for (var i = 0; i < ReadCount; i++)
        {
            Keccak k = default;
            BinaryPrimitives.WriteInt32LittleEndian(k.BytesAsSpan, i);

            var key = Key.Raw(NibblePath.FromKey(k), DataType.StorageCell, NibblePath.FromKey(k));
            var written = key.WriteTo(span);

            var hash = Blockchain.GetHash(key);
            _bigDictionary.Set(written, hash, written, 0);
        }
    }

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

    [Benchmark]
    public int Read_a_lot()
    {
        var length = 0;

        Span<byte> span = stackalloc byte[128];

        for (var i = 0; i < ReadCount; i++)
        {
            Keccak k = default;
            BinaryPrimitives.WriteInt32LittleEndian(k.BytesAsSpan, i);

            var path = NibblePath.FromKey(k);
            var key = Key.Raw(path, DataType.StorageCell, path);
            var written = key.WriteTo(span);

            var hash = Blockchain.GetHash(key);
            if (_bigDictionary.TryGet(written, hash, out var result))
            {
                length += result.Length;
            }
        }

        return length;
    }
}
