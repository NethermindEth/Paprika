using System.Buffers.Binary;
using System.Runtime.InteropServices;
using Paprika.Crypto;
using Paprika.Data;

namespace Paprika.Merkle;

public enum NodeType : byte
{
    Leaf,
    Extension,
    Branch
}

public ref struct MerkleNode
{

}

[StructLayout(LayoutKind.Explicit, Pack = 1, Size = 35)]
public readonly ref struct Branch
{
    public static int MaxByteLength => 35;

    private const int NibbleBitSetSize = sizeof(ushort);

    [FieldOffset(0)]
    private readonly MerkleNodeHeader _header;

    [FieldOffset(1)]
    private readonly ushort _nibbleBitSet;

    [FieldOffset(3)]
    private readonly Keccak _keccak;

    public bool IsDirty => _header.IsDirty;
    public NodeType NodeType => NodeType.Branch;
    public Keccak Keccak => _keccak;

    // TODO: What interface do we want to expose for nibbles?
    // Options:
    // - `IEnumerable<byte>` with all nibbles is not possible
    // - `byte[]` with all nibbles
    // - `bool HasNibble(byte nibble)` to lookup a single nibble at a time
    public bool HasNibble(byte nibble) => (_nibbleBitSet & (1 << nibble)) != 0;

    public Branch(MerkleNodeHeader header, ushort nibbleBitSet, Keccak keccak)
    {
        if (header.NodeType != NodeType.Branch)
        {
            throw new ArgumentException($"Expected Header with {nameof(Merkle.NodeType)} {nameof(NodeType.Branch)}, got {header.NodeType}");
        }

        _header = header;
        _nibbleBitSet = nibbleBitSet;
        _keccak = keccak;
    }

    public Branch(ushort nibbleBitSet, Keccak keccak)
    {
        _header = new MerkleNodeHeader(NodeType.Branch);
        _nibbleBitSet = nibbleBitSet;
        _keccak = keccak;
    }

    public Span<byte> WriteTo(Span<byte> output)
    {
        var leftover = _header.WriteTo(output);

        BinaryPrimitives.WriteUInt16LittleEndian(leftover, _nibbleBitSet);
        leftover = leftover.Slice(NibbleBitSetSize);

        leftover = _keccak.WriteTo(leftover);

        return leftover;
    }

    public static ReadOnlySpan<byte> ReadFrom(ReadOnlySpan<byte> source, out Branch branch)
    {
        var leftover = MerkleNodeHeader.ReadFrom(source, out var header);

        var nibbleBitSet = BinaryPrimitives.ReadUInt16LittleEndian(leftover);
        leftover = leftover.Slice(NibbleBitSetSize);

        leftover = Keccak.ReadFrom(leftover, out var keccak);

        branch = new Branch(header, nibbleBitSet, keccak);

        return leftover;
    }

    public bool Equals(in Branch other)
    {
        return _header.Equals(other._header)
               && _nibbleBitSet.Equals(other._nibbleBitSet)
               && _keccak.Equals(other._keccak);
    }

    public override string ToString() =>
        $"{nameof(Branch)} {{ " +
        $"{nameof(_header)}: {_header.ToString()}, " +
        $"{nameof(_nibbleBitSet)}: {_nibbleBitSet}, " +
        $"{nameof(_keccak)}: {_keccak} " +
        $"}}";
}

public readonly ref struct Leaf
{
    public int MaxByteLength => MerkleNodeHeader.Size + _path.MaxByteLength + Keccak.Size;

    private readonly MerkleNodeHeader _header;
    private readonly NibblePath _path;
    private readonly Keccak _keccak;

    public bool IsDirty => _header.IsDirty;
    public NodeType NodeType => NodeType.Leaf;

    public NibblePath NibblePath => _path;
    public Keccak Keccak => _keccak;

    public Leaf(MerkleNodeHeader header, NibblePath path, Keccak keccak)
    {
        _header = header;
        _path = path;
        _keccak = keccak;
    }

    public Leaf(NibblePath path, Keccak keccak)
    {
        _header = new MerkleNodeHeader(NodeType.Leaf);
        _path = path;
        _keccak = keccak;
    }

    public Span<byte> WriteTo(Span<byte> output)
    {
        var leftover = _header.WriteTo(output);
        leftover = _path.WriteToWithLeftover(leftover);
        leftover = _keccak.WriteTo(leftover);

        return leftover;
    }

    public static ReadOnlySpan<byte> ReadFrom(ReadOnlySpan<byte> source, out Leaf leaf)
    {
        var leftover = MerkleNodeHeader.ReadFrom(source, out var header);
        leftover = NibblePath.ReadFrom(leftover, out var path);
        leftover = Keccak.ReadFrom(leftover, out var keccak);

        leaf = new Leaf(header, path, keccak);
        return leftover;
    }

    public bool Equals(in Leaf other) =>
        _header.Equals(other._header)
        && _path.Equals(other._path)
        && _keccak.Equals(other._keccak);

    public override string ToString() =>
        $"{nameof(Leaf)} {{ " +
        $"{nameof(_header)}: {_header.ToString()}, " +
        $"{nameof(_path)}: {_path.ToString()}, " +
        $"{nameof(_keccak)}: {_keccak} " +
        $"}}";
}

public readonly ref struct Extension
{
    public int MaxByteLength => MerkleNodeHeader.Size + _path.MaxByteLength;

    private readonly MerkleNodeHeader _header;
    private readonly NibblePath _path;

    public bool IsDirty => _header.IsDirty;
    public NodeType NodeType => NodeType.Extension;

    public NibblePath NibblePath => _path;

    public Extension(MerkleNodeHeader header, NibblePath path)
    {
        _header = header;
        _path = path;
    }

    public Extension(NibblePath path)
    {
        _header = new MerkleNodeHeader(NodeType.Extension);
        _path = path;
    }

    public Span<byte> WriteTo(Span<byte> output)
    {
        var leftover = _header.WriteTo(output);
        leftover = _path.WriteToWithLeftover(leftover);

        return leftover;
    }

    public static ReadOnlySpan<byte> ReadFrom(ReadOnlySpan<byte> source, out Extension extension)
    {
        var leftover = MerkleNodeHeader.ReadFrom(source, out var header);
        leftover = NibblePath.ReadFrom(leftover, out var path);

        extension = new Extension(header, path);
        return leftover;
    }

    public bool Equals(in Extension other) =>
        _header.Equals(other._header)
        && _path.Equals(other._path);

    public override string ToString() =>
        $"{nameof(Extension)} {{ " +
        $"{nameof(_header)}: {_header.ToString()}, " +
        $"{nameof(_path)}: {_path.ToString()}, " +
        $"}}";
}

[StructLayout(LayoutKind.Explicit, Pack = 1, Size = Size)]
public readonly struct MerkleNodeHeader
{
    public const int Size = sizeof(byte);

    private const byte IsDirtyMask = 0b0001;
    private const byte NodeTypeMask = 0b0110;

    [FieldOffset(0)]
    private readonly byte _header;

    public bool IsDirty => (_header & IsDirtyMask) != 0;
    public NodeType NodeType => (NodeType)((_header & NodeTypeMask) >> 1);

    public MerkleNodeHeader(NodeType nodeType, bool isDirty = true)
    {
        _header = (byte)((byte)nodeType << 1 | (isDirty ? IsDirtyMask : 0));
    }

    public Span<byte> WriteTo(Span<byte> output)
    {
        output[0] = _header;
        return output.Slice(Size);
    }

    public static ReadOnlySpan<byte> ReadFrom(ReadOnlySpan<byte> source, out MerkleNodeHeader header)
    {
        var isDirty = (source[0] & IsDirtyMask) != 0;
        var nodeType = (NodeType)((source[0] & NodeTypeMask) >> 1);
        header = new MerkleNodeHeader(nodeType, isDirty);

        return source.Slice(Size);
    }

    public bool Equals(in MerkleNodeHeader other)
    {
        return _header.Equals(other._header);
    }

    public override string ToString() =>
        $"{nameof(MerkleNodeHeader)} {{ " +
        $"{nameof(IsDirty)}: {IsDirty}, " +
        $"{nameof(NodeType)}: {NodeType} " +
        $"}}";
}
