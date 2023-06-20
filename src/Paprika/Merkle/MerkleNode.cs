using System.Runtime.InteropServices;
using Paprika.Crypto;

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

[StructLayout(LayoutKind.Explicit, Pack = 1, Size = MaxSize)]
public readonly ref struct Branch
{
    public const int MaxSize = 35;
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

        BitConverter.TryWriteBytes(leftover, _nibbleBitSet);
        leftover = leftover.Slice(NibbleBitSetSize);

        Keccak.Span.CopyTo(leftover);
        leftover = leftover.Slice(Keccak.Size);

        return leftover;
    }

    public static ReadOnlySpan<byte> ReadFrom(ReadOnlySpan<byte> source, out Branch branch)
    {
        MerkleNodeHeader.ReadFrom(source, out var header);
        var nibbleBitSet = BitConverter.ToUInt16(source.Slice(1));
        var keccak = new Keccak(source.Slice(3));
        branch = new Branch(header, nibbleBitSet, keccak);

        return source.Slice(MaxSize);
    }
}

[StructLayout(LayoutKind.Explicit, Pack = 1, Size = MaxSize)]
public readonly struct MerkleNodeHeader
{
    public const int MaxSize = sizeof(byte);

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
        return output.Slice(1);
    }

    public static ReadOnlySpan<byte> ReadFrom(ReadOnlySpan<byte> source, out MerkleNodeHeader header)
    {
        var isDirty = (source[0] & IsDirtyMask) != 0;
        var nodeType = (NodeType)((source[0] & NodeTypeMask) >> 1);
        header = new MerkleNodeHeader(nodeType, isDirty);

        return source.Slice(1);
    }
}
