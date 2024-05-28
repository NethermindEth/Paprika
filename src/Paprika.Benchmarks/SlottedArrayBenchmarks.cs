using System.Buffers.Binary;
using System.Runtime.CompilerServices;
using BenchmarkDotNet.Attributes;
using Paprika.Crypto;
using Paprika.Data;
using Paprika.Store;

namespace Paprika.Benchmarks;

[DisassemblyDiagnoser(maxDepth: 2)]
public class SlottedArrayBenchmarks
{
    private readonly byte[] _writtenLittleEndian = new byte[Page.PageSize];
    private readonly byte[] _writtenBigEndian = new byte[Page.PageSize];
    private readonly byte[] _writable = new byte[Page.PageSize];
    private readonly int _to;

    private readonly byte[] _hashCollisions = new byte[Page.PageSize];
    private readonly int _hashCollisionsLength;
    private static readonly byte[] HashCollisionValue = new byte[13];

    private readonly byte[] _copy0 = new byte[Page.PageSize];
    private readonly byte[] _copy1 = new byte[Page.PageSize];

    public SlottedArrayBenchmarks()
    {
        var little = new SlottedArray(_writtenLittleEndian);
        var big = new SlottedArray(_writtenBigEndian);

        Span<byte> key = stackalloc byte[4];

        // Big and small endian tests
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

        // Hash collisions tests
        var zeroes = NibblePath.FromKey(Keccak.Zero);
        var hashCollisions = new SlottedArray(_hashCollisions);

        _hashCollisionsLength = 0;

        while (hashCollisions.TrySet(zeroes.SliceTo(_hashCollisionsLength), HashCollisionValue))
        {
            _hashCollisionsLength++;
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

    [Benchmark]
    public int Hash_collisions()
    {
        var map = new SlottedArray(_hashCollisions);
        var zeroes = NibblePath.FromKey(Keccak.Zero);

        var length = 0;

        for (var i = 0; i < _hashCollisionsLength; i++)
        {
            if (map.TryGet(zeroes.SliceTo(i), out var value))
            {
                length += value.Length;
            }
        }

        return length;
    }

    [Benchmark]
    public int EnumerateAll()
    {
        var map = new SlottedArray(_writtenLittleEndian);

        var length = 0;
        foreach (var item in map.EnumerateAll())
        {
            length += item.Key.Length;
            length += item.RawData.Length;
        }

        return length;
    }

    [Benchmark]
    public int Move_to_keys()
    {
        var map = new SlottedArray(_writtenLittleEndian);

        _copy0.AsSpan().Clear();
        _copy1.AsSpan().Clear();

        var map0 = new SlottedArray(_copy0);
        var map1 = new SlottedArray(_copy1);

        map.MoveNonEmptyKeysTo(new MapSource(map0, map1));

        return map.Count + map0.Count + map1.Count;
    }

    [Benchmark(OperationsPerInvoke = 4)]
    [Arguments(0)]
    [Arguments(1)]
    [Arguments(62)]
    [Arguments(63)]
    [Arguments(64)]
    public int UnPrepareKey(int sliceFrom)
    {
        var key = NibblePath.FromKey(Keccak.EmptyTreeHash).SliceFrom(sliceFrom);
        
        var map = new SlottedArray(stackalloc byte[256]);
        map.TrySet(key, ReadOnlySpan<byte>.Empty);

        var length = 0;
        
        foreach (var item in map.EnumerateAll())
        {
            length += item.Key.Length;
        }
        
        foreach (var item in map.EnumerateAll())
        {
            length += item.Key.Length;
        }
        
        foreach (var item in map.EnumerateAll())
        {
            length += item.Key.Length;
        }
        
        foreach (var item in map.EnumerateAll())
        {
            length += item.Key.Length;
        }

        return length;
    }
}