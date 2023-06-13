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
    public const uint NoCache = 0;
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
        int position;
        while ((position = _hashes.IndexOf(hash)) != IndexOfNotFound)
        {
            var entry = GetEntry(position);
            var path = key.Path;

            // ReSharper disable once StackAllocInsideLoop, highly unlikely as this should be only one hit
            Span<byte> destination = stackalloc byte[path.MaxByteLength + TypeBytes + key.AdditionalKey.Length];
            var prefix = GeneratePrefix(key, path, destination);

            if (entry.StartsWith(prefix))
            {
                var dataLength = entry[prefix.Length];
                value = entry.Slice(prefix.Length + LengthOfLength, dataLength);
                return true;
            }
        }

        value = default;
        return false;
    }

    private Span<byte> GetEntry(int position) => _entries.Slice(position * EntrySize, EntrySize);

    public bool TrySet(uint hash, in Key key, ReadOnlySpan<byte> value)
    {
        // no collision detection, just append at the first empty
        var position = _hashes.IndexOf(NoCache);
        if (position == IndexOfNotFound)
        {
            return false;
        }

        _hashes[position] = hash;

        var path = key.Path;
        Span<byte> destination = stackalloc byte[path.MaxByteLength + TypeBytes + key.AdditionalKey.Length];
        var prefix = GeneratePrefix(key, path, destination);

        // get the entry
        var entry = GetEntry(position);

        // copy to the entry
        prefix.CopyTo(entry);
        entry[prefix.Length] = (byte)value.Length;
        value.CopyTo(entry.Slice(prefix.Length + LengthOfLength));

        return true;
    }

    public static uint GetHash(in Key key)
    {
        if (CanBeCached(key))
        {
            var path = TrimPath(key);

            Span<byte> destination = stackalloc byte[path.MaxByteLength + TypeBytes + key.AdditionalKey.Length];

            var prefix = GeneratePrefix(key, path, destination);

            return XxHash32.HashToUInt32(prefix) | HasCache;
        }

        return NoCache;
    }

    private static Span<byte> GeneratePrefix(in Key key, NibblePath sliced, Span<byte> destination)
    {
        var leftover = sliced.WriteToWithLeftover(destination);
        leftover[0] = (byte)key.Type;
        key.AdditionalKey.CopyTo(leftover.Slice(TypeBytes));

        var written = destination.Slice(0,
            destination.Length - leftover.Length + TypeBytes + key.AdditionalKey.Length);
        return written;
    }

    private static NibblePath TrimPath(in Key key) => key.Path.SliceFrom(key.Path.Length - MinimumPathLength);

    public static bool CanBeCached(in Key key) => key.Path.Length >= MinimumPathLength;
}