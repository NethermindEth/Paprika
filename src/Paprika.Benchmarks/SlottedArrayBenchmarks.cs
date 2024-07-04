using System.Numerics;
using BenchmarkDotNet.Attributes;
using Paprika.Crypto;
using Paprika.Data;
using Paprika.Store;
using static System.Buffers.Binary.BinaryPrimitives;

namespace Paprika.Benchmarks;

[DisassemblyDiagnoser(maxDepth: 2)]
public class SlottedArrayBenchmarks
{
    private readonly byte[] _onePage = new byte[Page.PageSize];

    // defragmentation
    private readonly byte[] _defragmentation = new byte[Page.PageSize];
    private readonly byte[] _defragmentationCopy = new byte[Page.PageSize];
    private readonly byte[] _defragmentationValue = new byte[64];
    private readonly ushort _writtenTo;

    private readonly byte[] _writtenLittleEndian = new byte[Page.PageSize];
    private readonly byte[] _writtenBigEndian = new byte[Page.PageSize];
    private readonly byte[] _writable = new byte[Page.PageSize];
    private readonly int _to;

    // hash collisions are fixed in size to make them comparable
    private readonly byte[] _hashCollisions = new byte[Page.PageSize];
    private const int HashCollisionsCount = NibblePath.KeccakNibbleCount;
    private static readonly byte[] HashCollisionValue = new byte[13];

    private readonly byte[] _copy0 = new byte[Page.PageSize];
    private readonly byte[] _copy1 = new byte[Page.PageSize];

    public SlottedArrayBenchmarks()
    {
        // Big and small endian tests
        {
            var little = new SlottedArray(_writtenLittleEndian);
            var big = new SlottedArray(_writtenBigEndian);

            Span<byte> key = stackalloc byte[4];


            while (true)
            {
                WriteInt32LittleEndian(key, _to);
                if (little.TrySet(NibblePath.FromKey(key), key) == false)
                {
                    // filled
                    break;
                }

                WriteInt32BigEndian(key, _to);
                if (big.TrySet(NibblePath.FromKey(key), key) == false)
                {
                    // filled
                    break;
                }

                _to++;
            }
        }

        // Hash collisions tests
        {
            var zeroes = NibblePath.FromKey(Keccak.Zero);
            var hashCollisions = new SlottedArray(_hashCollisions);

            for (var i = 0; i <= HashCollisionsCount; i++)
            {
                if (!hashCollisions.TrySet(zeroes.SliceTo(i), HashCollisionValue))
                {
                    throw new Exception($"No place to set hash collision at {i}");
                }
            }
        }

        // Defragmentation
        {
            var map = new SlottedArray(_defragmentation);
            ushort i = 0;
            Span<byte> key = stackalloc byte[2];

            // Set as many as possible
            while (map.TrySet(NibblePath.FromKey(key), _defragmentationValue))
            {
                i++;
                WriteUInt16LittleEndian(key, i);
            }

            _writtenTo = i;
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
            WriteInt32LittleEndian(key, i);
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
            WriteInt32LittleEndian(key, i);
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
            WriteInt32BigEndian(key, i);
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
            WriteInt32LittleEndian(key, i);
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

        for (var i = 0; i < HashCollisionsCount; i++)
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

    /// <summary>
    /// Multiple rounds of setting and deleting to ensure that tombstones do not impact the search nor insert.
    /// Increasing values are used so that slot cannot be easily reused.
    /// </summary>
    [Benchmark]
    public void Set_And_Delete()
    {
        const int count = 80;

        Span<byte> data = stackalloc byte[count];
        var a = NibblePath.FromKey(stackalloc byte[] { 12, 34, 98 });
        var b = NibblePath.FromKey(stackalloc byte[] { 78, 34, 35 });

        var map = new SlottedArray(_onePage);
        map.Clear();

        // init by setting a
        map.TrySet(a, ReadOnlySpan<byte>.Empty);

        for (int i = 1; i < count; i++)
        {
            var d = data[..i];

            map.TrySet(b, d);
            map.Delete(a); // delete previous a, b above prohibits collect tombstones
            map.TrySet(a, d); // set new
            map.Delete(b); // delete previous b, a above prohibits collect tombstones
        }
    }

    private const int NonVectorizedOps = 16;
    [Benchmark(OperationsPerInvoke = NonVectorizedOps)]
    [Arguments(1)]
    [Arguments(3)]
    [Arguments(5)]
    [Arguments(7)]
    [Arguments(9)]
    [Arguments(15)]
    [Arguments(17)]
    public int Non_vectorizable_items_count(int count)
    {
        // IndexOf is vectorized.
        // The smallest vectorized size is Vector128 which is 16 bytes, which is 8 ushorts.
        // The biggest on this machine is 32 bytes which is 16 ushorts.
        // Test around these boundaries
        var path = NibblePath.FromKey(stackalloc byte[] { 12, 34, 45, 78, 91, 14 });

        // Setup
        var map = new SlottedArray(_onePage);
        map.Clear();

        for (int i = 0; i < count; i++)
        {
            map.TrySet(path.SliceTo(i + 1), ReadOnlySpan<byte>.Empty);
        }

        var notFound = path;

        var sum = 0;
        for (var i = 0; i < NonVectorizedOps / 2; i++)
        {
            if (map.TryGet(notFound, out _)) sum++;
            if (map.TryGet(notFound, out _)) sum++;
        }

        return sum;
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

    private const int DefragmentOpsCount = 4;

    [Benchmark(OperationsPerInvoke = DefragmentOpsCount)]
    public void Defragment_first_key_deleted()
    {
        _defragmentation.CopyTo(_defragmentationCopy.AsSpan());

        var map = new SlottedArray(_defragmentationCopy);

        Span<byte> key = stackalloc byte[2];
        var i = _writtenTo;

        // Delete & defragment
        for (ushort j = 0; j < DefragmentOpsCount; j++)
        {
            // Delete first
            WriteUInt16LittleEndian(key, j);
            map.Delete(NibblePath.FromKey(key));

            // Encode new key and set
            WriteUInt16LittleEndian(key, i++);
            map.TrySet(NibblePath.FromKey(key), _defragmentationValue);
        }
    }

    [Benchmark(OperationsPerInvoke = DefragmentOpsCount)]
    public void Defragment_last_key_deleted()
    {
        _defragmentation.CopyTo(_defragmentationCopy.AsSpan());

        var map = new SlottedArray(_defragmentationCopy);

        Span<byte> key = stackalloc byte[2];
        var last = (ushort)(_writtenTo - 1);

        // Delete & defragment
        for (ushort j = 0; j < DefragmentOpsCount; j++)
        {
            // Delete first
            WriteUInt16LittleEndian(key, last);
            map.Delete(NibblePath.FromKey(key));

            // Encode new key and set
            WriteUInt16LittleEndian(key, last++);
            map.TrySet(NibblePath.FromKey(key), _defragmentationValue);
        }
    }
}