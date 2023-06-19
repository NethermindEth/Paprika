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

[StructLayout(LayoutKind.Explicit, Pack = 1, Size = Size)]
public readonly struct Branch
{
    // Branch: 1 byte for type + dirty, 2 bytes for nibbles, 32 bytes for keccak
    private const int Size = 35;
    private const byte IsDirtyFlag = 0b1000;
    private const byte NodeTypeFlag = 0b0110;

    [FieldOffset(0)]
    private readonly byte _header;

    [FieldOffset(1)]
    private readonly ushort _nibbles;

    [FieldOffset(3)]
    private readonly Keccak _keccak;

    public bool IsDirty => (_header & IsDirtyFlag) != 0;
    public NodeType Type => (NodeType)((_header & NodeTypeFlag) >> 1);

    public IEnumerable<byte> Nibbles()
    {
        var mask = 1;
        for (byte i = 0; i < 16; i++, mask <<= 1)
        {
            if ((_nibbles & mask) != 0)
            {
                yield return i;
            }
        }
    }

    public Keccak Keccak => _keccak;

    // TODO: Should we expose ushort? What about a Span of two bytes?
    public Branch(ushort nibbles)
    {
        _header = (byte)NodeType.Branch << 1 | IsDirtyFlag;
        _nibbles = nibbles;
    }
}

[StructLayout(LayoutKind.Explicit, Pack = 1, Size = Size)]
public readonly struct Extension
{
    // Extension: 1 byte for type + dirty, var-length for path (usually it will be a nibble or a few)
    private const int Size = 33;
    private const byte IsDirtyFlag = 0b1000;
    private const byte NodeTypeFlag = 0b0110;

    [FieldOffset(0)]
    private readonly byte _header;

    public NibblePath Path => default;

    [FieldOffset(1)]
    private readonly Keccak _keccak;

    public Keccak Keccak => _keccak;

    public bool IsDirty => (_header & IsDirtyFlag) != 0;
    public NodeType Type => (NodeType)((_header & NodeTypeFlag) >> 1);

    public Extension(NibblePath path)
    {
        _header = (byte)NodeType.Extension << 1 | IsDirtyFlag;
    }
}
