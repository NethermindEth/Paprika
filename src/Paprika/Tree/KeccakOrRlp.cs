namespace Paprika.Tree;

public ref struct KeccakOrRlp
{
    public enum Type : byte
    {
        Keccak = 0,
        Rlp = 1
    }

    public Type DataType { get; }
    public Span<byte> Data { get; }

    public KeccakOrRlp(Type dataType, Span<byte> data)
    {
        DataType = dataType;
        Data = data;
    }
}