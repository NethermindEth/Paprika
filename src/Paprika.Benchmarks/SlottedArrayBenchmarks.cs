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

    private NibblePath GetKey(byte i, bool odd)
    {
        var span = new Span<byte>(_keys, Page.PageSize);
        var slice = span.Slice(i * BytesPerKey, BytesPerKey);

        return NibblePath.FromKey(slice, odd ? 1 : 0, 4);
    }
}