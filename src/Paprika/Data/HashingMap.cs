using System.Buffers;
using System.IO.Hashing;
using System.Runtime.InteropServices;
using Paprika.Crypto;

namespace Paprika.Data;

/// <summary>
/// An in page map of key->value, used for storing values in upper levels of Paprika tree using a stable hashing.
/// </summary>
/// <remarks>
/// The path is trimmed only for hashing purposes so that on all levels the same hash of the entry is used.
/// This allows nice copying to the next level, without hash recalculation.
///
/// For 
/// </remarks>
public readonly ref struct HashingMap
{
    /// <summary>
    /// A value set so that there top <see cref="SkipInitialNibbles"/> - 1 levels of the tree have a chance to be
    /// cached in this map.
    /// </summary>
    private const int SkipInitialNibbles = 6;

    private const int MinimumPathLength = Keccak.Size * NibblePath.NibblePerByte - SkipInitialNibbles;
    public const uint Null = 0;
    private const uint HasCache = 0b1000_0000_0000_0000;

    private const int TypeBytes = 1;
    private const int LengthOfLength = 1;
    private const int MaxValueLength = 32;

    // NibblePath.FullKeccakByteLength + TypeBytes + LengthPrefix + Keccak.Size + LengthPrefix + MaxValueLength;
    private const int EntrySize = 88;
    private const int HashSize = sizeof(uint);
    private const int TotalEntrySize = EntrySize + HashSize;
    private const int IndexOfNotFound = -1;
    public const int MinSize = TotalEntrySize;

    private readonly Span<uint> _hashes;
    private readonly Span<byte> _entries;

    public HashingMap(Span<byte> buffer)
    {
        var count = buffer.Length / TotalEntrySize;

        _hashes = MemoryMarshal.Cast<byte, uint>(buffer).Slice(0, count);
        _entries = buffer.Slice(count * HashSize);
    }

    public bool TryGet(uint hash, in Key key, out ReadOnlySpan<byte> value)
    {
        var hashes = _hashes;

        var offset = 0;
        int index;

        while ((index = hashes.IndexOf(hash)) != IndexOfNotFound)
        {
            offset += index;

            var entry = GetEntry(offset);

            // ReSharper disable once StackAllocInsideLoop, highly unlikely as this should be only one hit
            Span<byte> destination = stackalloc byte[key.Path.MaxByteLength + TypeBytes + LengthOfLength + key.AdditionalKey.Length];
            var prefix = WritePrefix(key, destination);

            if (entry.StartsWith(prefix))
            {
                var dataLength = entry[prefix.Length];
                value = entry.Slice(prefix.Length + LengthOfLength, dataLength);
                return true;
            }

            if (index + 1 >= hashes.Length)
            {
                // the span is empty and there's not place to move forward
                break;
            }

            hashes = hashes.Slice(index + 1);
            offset += 1;
        }

        value = default;
        return false;
    }

    private Span<byte> GetEntry(int position) => _entries.Slice(position * EntrySize, EntrySize);

    public bool TrySet(uint hash, in Key key, ReadOnlySpan<byte> value)
    {
        // no collision detection, just append at the first empty
        var position = _hashes.IndexOf(Null);
        if (position == IndexOfNotFound)
        {
            return false;
        }

        _hashes[position] = hash;

        Span<byte> destination = stackalloc byte[GetPrefixAllocationSize(key)];
        var prefix = WritePrefix(key, destination);

        // get the entry
        var entry = GetEntry(position);

        // copy to the entry
        prefix.CopyTo(entry);
        entry[prefix.Length] = (byte)value.Length;
        value.CopyTo(entry.Slice(prefix.Length + LengthOfLength));

        return true;
    }

    private static int GetPrefixAllocationSize(in Key key) =>
        key.Path.MaxByteLength + // path 
        TypeBytes +  // type
        LengthOfLength + key.AdditionalKey.Length; // additional key

    public static uint GetHash(in Key key)
    {
        if (CanBeCached(key))
        {
            var k = TrimToRightPathLenght(key);

            Span<byte> destination = stackalloc byte[GetPrefixAllocationSize(key)];

            var prefix = WritePrefix(k, destination);

            return XxHash32.HashToUInt32(prefix) | HasCache;
        }

        return Null;
    }

    public void Clear()
    {
        _hashes.Clear();

        // it's sufficient to clear the caches
        //_entries.Clear();
    }

    private static Span<byte> WritePrefix(in Key key, Span<byte> destination)
    {
        var leftover = key.Path.WriteToWithLeftover(destination);
        leftover[0] = (byte)key.Type;
        leftover[TypeBytes] = (byte)key.AdditionalKey.Length;

        key.AdditionalKey.CopyTo(leftover.Slice(TypeBytes + LengthOfLength));

        var written = destination.Slice(0,
            destination.Length - leftover.Length + TypeBytes + LengthOfLength + key.AdditionalKey.Length);
        return written;
    }
    //
    // private static Enumerator.Item ParseItem(ReadOnlySpan<byte> entry)
    // {
    //     var leftover = NibblePath.ReadFrom(entry, out var path);
    //     var type = (DataType)leftover[0];
    //
    //     var leftover = sliced.WriteToWithLeftover(destination);
    //     leftover[0] = (byte)key.Type;
    //     key.AdditionalKey.CopyTo(leftover.Slice(TypeBytes));
    //
    //     var written = destination.Slice(0,
    //         destination.Length - leftover.Length + TypeBytes + key.AdditionalKey.Length);
    //     return written;
    // }
    //
    // public Enumerator GetEnumerator() => new(this);
    //
    // public ref struct Enumerator
    // {
    //     /// <summary>The map being enumerated.</summary>
    //     private readonly HashingMap _map;
    //
    //     private readonly int _count;
    //
    //     /// <summary>The next index to yield.</summary>
    //     private int _index;
    //
    //     internal Enumerator(HashingMap map)
    //     {
    //         _map = map;
    //         _count = map._hashes.IndexOf(Null);
    //     }
    //
    //     /// <summary>Advances the enumerator to the next element of the span.</summary>
    //     public bool MoveNext()
    //     {
    //         int index = _index + 1;
    //         if (index < _count)
    //         {
    //             _index = index;
    //             return true;
    //         }
    //
    //         return false;
    //     }
    //
    //     public Item Current
    //     {
    //         get
    //         {
    //             var hash = _map._hashes[_index];
    //             var entry = _map.GetEntry(_index);
    //
    //             ref var slot = ref _map._slots[_index];
    //             var span = _map.GetSlotPayload(ref slot);
    //
    //             ReadOnlySpan<byte> data;
    //             NibblePath path;
    //
    //             // path rebuilding
    //             Span<byte> nibbles = stackalloc byte[3];
    //             var count = slot.DecodeNibblesFromPrefix(nibbles);
    //
    //             if (count == 0)
    //             {
    //                 // no nibbles stored in the slot, read as is.
    //                 data = NibblePath.ReadFrom(span, out path);
    //             }
    //             else
    //             {
    //                 // there's at least one nibble extracted
    //                 var raw = NibblePath.RawExtract(span);
    //                 data = span.Slice(raw.Length);
    //
    //                 const int space = 2;
    //
    //                 var bytes = _bytes.AsSpan(0, raw.Length + space); //big enough to handle all cases
    //
    //                 // copy forward enough to allow negative pointer arithmetics
    //                 var pathDestination = bytes.Slice(space);
    //                 raw.CopyTo(pathDestination);
    //
    //                 // Terribly unsafe region!
    //                 // Operate on the copy, pathDestination, as it will be overwritten with unsafe ref.
    //                 NibblePath.ReadFrom(pathDestination, out path);
    //
    //                 var countOdd = (byte)(count & 1);
    //                 for (var i = 0; i < count; i++)
    //                 {
    //                     path.UnsafeSetAt(i - count - 1, countOdd, nibbles[i]);
    //                 }
    //
    //                 path = path.CopyWithUnsafePointerMoveBack(count);
    //             }
    //
    //             if (slot.Type == DataType.StorageCell)
    //             {
    //                 const int size = Keccak.Size;
    //                 var additionalKey = data.Slice(0, size);
    //                 return new Item(Key.StorageCell(path, additionalKey), data.Slice(size), slot.Type);
    //             }
    //
    //             return new Item(Key.Raw(path, slot.Type), data, slot.Type);
    //         }
    //     }
    //
    //     public void Dispose()
    //     {
    //         if (_bytes != null)
    //             ArrayPool<byte>.Shared.Return(_bytes);
    //     }
    //
    //     public readonly ref struct Item
    //     {
    //         public uint Hash { get; }
    //         public DataType Type { get; }
    //         public Key Key { get; }
    //         public ReadOnlySpan<byte> RawData { get; }
    //
    //         public Item(Key key, ReadOnlySpan<byte> rawData, DataType type)
    //         {
    //             Type = type;
    //             Key = key;
    //             RawData = rawData;
    //         }
    //     }
    // }

    private static Key TrimToRightPathLenght(in Key key) => key.SliceFrom(key.Path.Length - MinimumPathLength);

    public static bool CanBeCached(in Key key) => key.Path.Length >= MinimumPathLength;
}