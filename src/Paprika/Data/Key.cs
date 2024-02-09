using System.Diagnostics;
using System.IO.Hashing;
using System.Runtime.CompilerServices;
using Paprika.Crypto;
using Paprika.Store;

namespace Paprika.Data;

/// <summary>
/// Represents the key of the <see cref="SlottedArray"/>, by combining a path <see cref="NibblePath"/>,
/// a type <see cref="DataType"/> and a potential <see cref="StoragePath"/>.
/// </summary>
/// <remarks>
/// Use factory methods to create one.
/// </remarks>
public readonly ref partial struct Key
{
    public readonly NibblePath Path;
    public readonly DataType Type;
    public readonly NibblePath StoragePath;

    private Key(NibblePath path, DataType type, NibblePath storagePath)
    {
        Path = path;
        Type = type;
        StoragePath = storagePath;
    }

    /// <summary>
    /// To be used only by FixedMap internally. Builds the raw key
    /// </summary>
    public static Key Raw(NibblePath path, DataType type, NibblePath storagePath) => new(path, type, storagePath);

    /// <summary>
    /// Builds the key for <see cref="DataType.Account"/>.
    /// </summary>
    public static Key Account(NibblePath path) => new(path, DataType.Account, NibblePath.Empty);

    public static Key Account(in Keccak key) => Account(NibblePath.FromKey(key));

    /// <summary>
    /// Builds the key for <see cref="DataType.StorageCell"/>.
    /// </summary>
    /// <remarks>
    /// <paramref name="keccak"/> must be passed by ref, otherwise it will blow up the span!
    /// </remarks>
    public static Key StorageCell(NibblePath path, in Keccak keccak) =>
        new(path, DataType.StorageCell, NibblePath.FromKey(keccak));

    /// <summary>
    /// Builds the key for <see cref="DataType.StorageCell"/>.
    /// </summary>
    /// <remarks>
    /// <paramref name="keccak"/> must be passed by ref, otherwise it will blow up the span!
    /// </remarks>
    public static Key StorageCell(NibblePath path, ReadOnlySpan<byte> keccak) =>
        new(path, DataType.StorageCell, NibblePath.FromKey(keccak));

    [DebuggerStepThrough]
    public Key SliceFrom(int nibbles) => new(Path.SliceFrom(nibbles), Type, StoragePath);

    public bool Equals(in Key key)
    {
        return Type == key.Type && StoragePath.Equals(key.StoragePath) && Path.Equals(key.Path);
    }

    private const int TypeByteLength = 1;

    public int MaxByteLength => TypeByteLength + Path.MaxByteLength + StoragePath.MaxByteLength;

    /// <summary>
    /// Writes the span to the destination.
    /// </summary>
    /// <returns>The leftover.</returns>
    public Span<byte> WriteTo(Span<byte> destination)
    {
        destination[0] = (byte)Type;
        var leftover = Path.WriteToWithLeftover(destination.Slice(1));
        leftover = StoragePath.WriteToWithLeftover(leftover);

        var written = destination.Length - leftover.Length;
        return destination.Slice(0, written);
    }

    public static ReadOnlySpan<byte> ReadFrom(ReadOnlySpan<byte> source, out Key key)
    {
        var type = (DataType)source[0];
        var leftover = NibblePath.ReadFrom(source.Slice(1), out var path);
        leftover = NibblePath.ReadFrom(leftover, out var storageKey);
        key = new Key(path, type, storageKey);

        return leftover;
    }

    public bool IsAccountCompressed => ((Type & DataType.CompressedAccount) == DataType.CompressedAccount);

    public bool IsState => Type == DataType.Account ||
                           (Type == DataType.Merkle && Path.Length < NibblePath.KeccakNibbleCount);


    [SkipLocalsInit]
    public override int GetHashCode()
    {
        return (int)Type ^ Path.GetHashCode() ^ StoragePath.GetHashCode();
    }

    [SkipLocalsInit]
    public ulong GetHashCodeULong()
    {
        return unchecked((ulong)(((long)Path.GetHashCode() << 32) |
                                 (long)(StoragePath.GetHashCode() ^ (byte)Type)));
    }

    public override string ToString()
    {
        return $"{nameof(Path)}: {Path.ToString()}, " +
               $"{nameof(Type)}: {Type}, " +
               $"{nameof(StoragePath)}: {StoragePath.ToString()}";
    }

    /// <summary>
    /// The predicate over a key.
    /// </summary>
    public delegate bool Predicate(in Key key);
}