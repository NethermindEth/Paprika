using System.Runtime.InteropServices;
using BenchmarkDotNet.Attributes;
using Paprika.Crypto;
using Paprika.Data;
using Paprika.Store;

namespace Paprika.Benchmarks;

[DisassemblyDiagnoser(maxDepth: 2)]
public unsafe class SlottedArrayBenchmarks
{
    private const int KeyCount = 97;

    private const int
        BytesPerKey =
            3; // 3 repeated bytes allow to cut off the first nibble and still have a unique key. Also, allow storing some key leftover

    private readonly void* _keys;
    private readonly void* _map;

    // Hash colliding
    private const int HashCollidingKeyCount = 32;

    // Use first and last as opportunity to collide
    private const int BytesPerKeyHashColliding = 3;
    private readonly void* _hashCollidingKeys;
    private readonly void* _hashCollidingMap;

    public SlottedArrayBenchmarks()
    {
        // Create keys
        _keys = AllocAlignedPage();

        var span = new Span<byte>(_keys, Page.PageSize);
        for (byte i = 0; i < KeyCount; i++)
        {
            for (var j = 0; j < BytesPerKey; j++)
            {
                span[i * BytesPerKey + j] = i;
            }
        }

        // Map
        _map = AllocAlignedPage();
        Span<byte> value = stackalloc byte[1];

        var map = new SlottedArray(new Span<byte>(_map, Page.PageSize));
        for (byte i = 0; i < KeyCount; i++)
        {
            value[0] = i;
            if (map.TrySet(GetKey(i, false), value) == false)
            {
                throw new Exception("Not enough memory");
            }
        }

        // Hash colliding
        _hashCollidingKeys = AllocAlignedPage();

        // Create keys so that two consecutive ones share the hash.
        // This should make it somewhat realistic where there are some collisions but not a lot of them.
        var hashCollidingKeys = new Span<byte>(_hashCollidingKeys, Page.PageSize);
        for (byte i = 0; i < HashCollidingKeyCount; i++)
        {
            // 0th divide by 2 to collide
            hashCollidingKeys[i * BytesPerKeyHashColliding] = (byte)(i / 2);

            // 1th differentiate with the first
            hashCollidingKeys[i * BytesPerKeyHashColliding + 1] = i;

            // 2nd divide by 2 to collide
            hashCollidingKeys[i * BytesPerKeyHashColliding + 2] = (byte)(i / 2);
        }

        _hashCollidingMap = AllocAlignedPage();

        var hashColliding = new SlottedArray(new Span<byte>(_hashCollidingMap, Page.PageSize));
        for (byte i = 0; i < HashCollidingKeyCount; i++)
        {
            value[0] = i;
            if (hashColliding.TrySet(GetHashCollidingKey(i), value) == false)
            {
                throw new Exception("Not enough memory");
            }
        }

        return;

        static void* AllocAlignedPage()
        {
            const UIntPtr size = Page.PageSize;
            var memory = NativeMemory.AlignedAlloc(size, size);
            NativeMemory.Clear(memory, size);
            return memory;
        }
    }

    [Benchmark(OperationsPerInvoke = 4)]
    [Arguments((byte)1, false)]
    [Arguments((byte)15, false)]
    [Arguments((byte)16, false)]
    [Arguments((byte)31, false)]
    [Arguments((byte)32, false)]
    [Arguments((byte)47, false)]
    [Arguments((byte)48, false)]
    [Arguments((byte)63, false)]
    [Arguments((byte)64, false)]
    [Arguments((byte)95, false)]
    [Arguments((byte)KeyCount - 1, false)]
    public int TryGet(byte index, bool odd)
    {
        var map = new SlottedArray(new Span<byte>(_map, Page.PageSize));
        var key = GetKey(index, odd);

        var count = 0;
        if (map.TryGet(key, out _)) count += 1;
        if (map.TryGet(key, out _)) count += 1;
        if (map.TryGet(key, out _)) count += 1;
        if (map.TryGet(key, out _)) count += 1;
        return count;
    }

    [Benchmark(OperationsPerInvoke = 4)]
    [Arguments((byte)1)]
    [Arguments((byte)2)]
    [Arguments((byte)3)]
    [Arguments((byte)4)]
    [Arguments((byte)30)]
    [Arguments((byte)31)]
    public int TryGet_With_Hash_Collisions(byte index)
    {
        var map = new SlottedArray(new Span<byte>(_hashCollidingMap, Page.PageSize));
        var key = GetHashCollidingKey(index);

        var count = 0;
        if (map.TryGet(key, out _)) count += 1;
        if (map.TryGet(key, out _)) count += 1;
        if (map.TryGet(key, out _)) count += 1;
        if (map.TryGet(key, out _)) count += 1;
        return count;
    }

    [Benchmark(OperationsPerInvoke = 2)]
    [Arguments(0, 0)]
    [Arguments(0, 1)]
    [Arguments(1, 1)]
    [Arguments(0, 2)]
    [Arguments(1, 2)]
    [Arguments(0, 3)]
    [Arguments(1, 3)]
    [Arguments(0, 4)]
    [Arguments(1, 4)]
    [Arguments(0, 6)]
    [Arguments(1, 6)]
    [Arguments(0, 32)]
    [Arguments(1, 31)]
    [Arguments(1, 30)]
    public int Prepare_Key(int sliceFrom, int length)
    {
        var key = NibblePath.FromKey(Keccak.EmptyTreeHash).Slice(sliceFrom, length);

        // spin: 1
        var hash = SlottedArray.PrepareKeyForTests(key, out var preamble, out var trimmed);

        // spin: 2
        var hash2 = SlottedArray.PrepareKeyForTests(key, out var preamble2, out var trimmed2);

        return
            hash + preamble + trimmed.Length +
            hash2 + preamble2 + trimmed2.Length;
    }

    [Benchmark]
    public int EnumerateAll()
    {
        var map = new SlottedArray(new Span<byte>(_map, Page.PageSize));

        var length = 0;
        foreach (var item in map.EnumerateAll())
        {
            length += item.Key.Length;
            length += item.RawData.Length;
        }

        return length;
    }

    [Benchmark]
    [Arguments((byte)0)]
    [Arguments((byte)1)]
    public int EnumerateNibble(byte nibble)
    {
        var map = new SlottedArray(new Span<byte>(_map, Page.PageSize));

        var length = 0;
        foreach (var item in map.EnumerateNibble(nibble))
        {
            length += item.Key.Length;
            length += item.RawData.Length;
        }

        return length;
    }

    private NibblePath GetKey(byte i, bool odd)
    {
        var span = new Span<byte>(_keys, BytesPerKey * KeyCount);
        var slice = span.Slice(i * BytesPerKey, BytesPerKey);

        return NibblePath.FromKey(slice, odd ? 1 : 0, 4);
    }

    private NibblePath GetHashCollidingKey(byte i)
    {
        var span = new Span<byte>(_hashCollidingKeys, BytesPerKeyHashColliding * HashCollidingKeyCount);
        var slice = span.Slice(i * BytesPerKeyHashColliding, BytesPerKeyHashColliding);

        // Use full key
        return NibblePath.FromKey(slice, 0, BytesPerKeyHashColliding * NibblePath.NibblePerByte);
    }
}