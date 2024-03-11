using System.Buffers.Binary;
using BenchmarkDotNet.Attributes;
using Paprika.Data;
using Paprika.Store;

namespace Paprika.Benchmarks;

public class SlottedArrayBenchmarks
{
    private readonly byte[] _writtenLittleEndian = new byte[Page.PageSize];
    private readonly byte[] _writtenBigEndian = new byte[Page.PageSize];
    private readonly byte[] _writable = new byte[Page.PageSize];
    private readonly int _to;

    public SlottedArrayBenchmarks()
    {
        var little = new SlottedArray(_writtenLittleEndian);
        var big = new SlottedArray(_writtenBigEndian);

        Span<byte> key = stackalloc byte[4];

        // fill 
        while (true)
        {
            BinaryPrimitives.WriteInt32LittleEndian(key, _to);
            if (little.TrySet(NibblePath.FromKey(key), key) == false)
            {
                // filled
                break;
            }

            BinaryPrimitives.WriteInt32BigEndian(key, _to);
            if (big.TrySet(NibblePath.FromKey(key), key) == false)
            {
                // filled
                break;
            }

            _to++;
        }
    }

    [Benchmark]
    public int Write_whole_page_of_data()
    {
        _writable.AsSpan().Clear();
        var map = new SlottedArray(_writable);

        Span<byte> key = stackalloc byte[4];

        int count = 0;

        // fill 
        for (int i = 0; i < _to; i++)
        {
            BinaryPrimitives.WriteInt32LittleEndian(key, i);
            if (map.TrySet(NibblePath.FromKey(key), key))
            {
                count++;
            }
        }

        return count;
    }

    [Benchmark]
    public int Read_existing_keys_prefix_different()
    {
        var map = new SlottedArray(_writtenLittleEndian);
        Span<byte> key = stackalloc byte[4];

        var result = 0;

        // find all values
        for (var i = 0; i < _to; i++)
        {
            BinaryPrimitives.WriteInt32LittleEndian(key, i);
            if (map.TryGet(NibblePath.FromKey(key), out var data))
            {
                result += data.Length;
            }
        }

        return result;
    }

    [Benchmark]
    public int Read_existing_keys_suffix_different()
    {
        var map = new SlottedArray(_writtenBigEndian);
        Span<byte> key = stackalloc byte[4];

        var result = 0;

        // find all values
        for (var i = 0; i < _to; i++)
        {
            BinaryPrimitives.WriteInt32BigEndian(key, i);
            if (map.TryGet(NibblePath.FromKey(key), out var data))
            {
                result += data.Length;
            }
        }

        return result;
    }

    [Benchmark]
    public int Read_nonexistent_keys()
    {
        var map = new SlottedArray(_writtenLittleEndian);
        Span<byte> key = stackalloc byte[4];

        var result = 0;

        // miss all the next
        for (int i = _to; i < _to * 2; i++)
        {
            BinaryPrimitives.WriteInt32LittleEndian(key, i);
            if (map.TryGet(NibblePath.FromKey(key), out _) == false)
            {
                result += 1;
            }
        }

        return result;
    }
}
