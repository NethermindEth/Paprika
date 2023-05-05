using System.Buffers.Binary;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Disassemblers;
using Paprika.Pages;

namespace Paprika.Benchmarks;

public class FixedMapBenchmarks
{
    private readonly byte[] _data = new byte[Page.PageSize];
    private readonly int _to;

    public FixedMapBenchmarks()
    {
        var map = new FixedMap(_data);

        Span<byte> key = stackalloc byte[4];

        // fill 
        while (true)
        {
            BinaryPrimitives.WriteInt32LittleEndian(key, _to);
            var path = NibblePath.FromKey(key);
            if (map.TrySet(FixedMap.Key.Account(path), key) == false)
            {
                // filled
                break;
            }

            _to++;
        }
    }

    [Benchmark]
    public int Read_all_keys()
    {
        var map = new FixedMap(_data);
        Span<byte> key = stackalloc byte[4];
        for (var i = 0; i < _to; i++)
        {
            BinaryPrimitives.WriteInt32LittleEndian(key, i);
            var path = NibblePath.FromKey(key);
            if (map.TryGet(FixedMap.Key.Account(path), out var data) == false)
            {
                return i;
            }
        }

        return _to;
    }
}