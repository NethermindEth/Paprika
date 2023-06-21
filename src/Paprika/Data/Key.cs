using System.Diagnostics;
using Paprika.Crypto;
using Paprika.Store;

namespace Paprika.Data;

/// <summary>
/// Represents the key of the <see cref="NibbleBasedMap"/>, by combining a path <see cref="NibblePath"/>,
/// a type <see cref="DataType"/> and a potential <see cref="AdditionalKey"/>.
/// </summary>
/// <remarks>
/// Use factory methods to create one.
/// </remarks>
public readonly ref struct Key
{
    public readonly NibblePath Path;
    public readonly DataType Type;
    public readonly ReadOnlySpan<byte> AdditionalKey;

    private Key(NibblePath path, DataType type, ReadOnlySpan<byte> additionalKey)
    {
        Path = path;
        Type = type;
        AdditionalKey = additionalKey;
    }

    /// <summary>
    /// To be used only by FixedMap internally. Builds the raw key
    /// </summary>
    public static Key Raw(NibblePath path, DataType type) =>
        new(path, type, ReadOnlySpan<byte>.Empty);

    /// <summary>
    /// Builds the key for <see cref="DataType.Account"/>.
    /// </summary>
    public static Key Account(NibblePath path) => new(path, DataType.Account, ReadOnlySpan<byte>.Empty);

    public static Key Account(in Keccak key) => Account(NibblePath.FromKey(key));

    /// <summary>
    /// Builds the key for <see cref="DataType.CodeHash"/>.
    /// </summary>
    public static Key CodeHash(NibblePath path) => new(path, DataType.CodeHash, ReadOnlySpan<byte>.Empty);

    /// <summary>
    /// Builds the key for <see cref="DataType.StorageRootHash"/>.
    /// </summary>
    public static Key StorageRootHash(NibblePath path) =>
        new(path, DataType.StorageRootHash, ReadOnlySpan<byte>.Empty);

    /// <summary>
    /// Builds the key for <see cref="DataType.StorageCell"/>.
    /// </summary>
    /// <remarks>
    /// <paramref name="keccak"/> must be passed by ref, otherwise it will blow up the span!
    /// </remarks>
    public static Key StorageCell(NibblePath path, in Keccak keccak) =>
        new(path, DataType.StorageCell, keccak.Span);

    /// <summary>
    /// Builds the key for <see cref="DataType.StorageCell"/>.
    /// </summary>
    /// <remarks>
    /// <paramref name="keccak"/> must be passed by ref, otherwise it will blow up the span!
    /// </remarks>
    public static Key StorageCell(NibblePath path, ReadOnlySpan<byte> keccak) =>
        new(path, DataType.StorageCell, keccak);

    /// <summary>
    /// Builds the key identifying the value of the <see cref="DbAddress"/> for the root of the storage tree.
    /// </summary>
    public static Key StorageTreeRootPageAddress(NibblePath path) =>
        new(path, DataType.StorageTreeRootPageAddress, ReadOnlySpan<byte>.Empty);

    /// <summary>
    /// Treat the additional key as the key and drop the additional notion.
    /// </summary>
    public static Key StorageTreeStorageCell(Key originalKey) =>
        new(NibblePath.FromKey(originalKey.AdditionalKey), DataType.StorageTreeStorageCell,
            ReadOnlySpan<byte>.Empty);

    /// <summary>
    /// Builds the key responsible for storing the encoded RLP or Keccak (if len(RLP) >= 32) for
    /// a node with the given <see cref="NibblePath"/>.
    /// </summary>
    public static Key KeccakOrRlp(NibblePath path) =>
        new(path, DataType.KeccakOrRlp, ReadOnlySpan<byte>.Empty);

    [DebuggerStepThrough]
    public Key SliceFrom(int nibbles) => new(Path.SliceFrom(nibbles), Type, AdditionalKey);

    public bool Equals(in Key key)
    {
        return Type == key.Type && AdditionalKey.SequenceEqual(key.AdditionalKey) && Path.Equals(key.Path);
    }

    public override string ToString()
    {
        return $"{nameof(Path)}: {Path.ToString()}, " +
               $"{nameof(Type)}: {Type}, " +
               $"{nameof(AdditionalKey)}: {AdditionalKey.ToHexString(false)}";
    }
}