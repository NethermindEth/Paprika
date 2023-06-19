using Paprika.Crypto;

namespace Paprika.Merkle;

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

    public static KeccakOrRlp WrapRlp(Span<byte> data)
    {
        var destination = new byte[32];

        if (data.Length < 32)
        {
            destination[0] = (byte)data.Length;
            data.CopyTo(destination[1..]);
            return new KeccakOrRlp(Type.Rlp, destination);
        }
        else
        {
            KeccakHash.ComputeHash(data, destination);
            return new KeccakOrRlp(Type.Keccak, destination);
        }
    }
}
