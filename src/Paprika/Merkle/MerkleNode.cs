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

[StructLayout(LayoutKind.Explicit, Pack = 1, Size = 35)]
public readonly ref struct Branch
{
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

    public Branch(ushort nibbleBitSet, Keccak keccak)
    {
        _header = new MerkleNodeHeader(NodeType.Branch);
        _nibbleBitSet = nibbleBitSet;
        _keccak = keccak;
    }
}

[StructLayout(LayoutKind.Explicit, Pack = 1, Size = Size)]
public readonly struct MerkleNodeHeader
{
    private const int Size = sizeof(byte);

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
}
