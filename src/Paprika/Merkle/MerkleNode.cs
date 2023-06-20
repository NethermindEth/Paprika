using System.Runtime.InteropServices;

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
