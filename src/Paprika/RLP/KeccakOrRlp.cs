using Paprika.Crypto;

namespace Paprika.RLP;

/// <summary>
/// Represents a result of encoding a node in Trie. If it's length is shorter than 32 bytes, then it's an RLP.
/// If it's equal or bigger, then it's its Keccak.
/// </summary>
public readonly ref struct KeccakOrRlp
{
    public enum Type : byte
    {
        Keccak = 0,
        Rlp = 1,
    }

    public readonly Type DataType;
    private readonly Keccak _keccak;

    public Span<byte> AsSpan() => _keccak.BytesAsSpan;

    private KeccakOrRlp(Type dataType, scoped Span<byte> data)
    {
        DataType = dataType;
        _keccak = new Keccak(data);
    }

    public static implicit operator KeccakOrRlp(Keccak keccak) => new(Type.Keccak, keccak.BytesAsSpan);

    public static KeccakOrRlp FromSpan(scoped Span<byte> data)
    {
        Span<byte> output = stackalloc byte[Keccak.Size];

        if (data.Length < 32)
        {
            output[0] = (byte)data.Length;
            data.CopyTo(output[1..]);
            return new KeccakOrRlp(Type.Rlp, output);
        }
        else
        {
            KeccakHash.ComputeHash(data, output);
            return new KeccakOrRlp(Type.Keccak, output);
        }
    }
}

public static class RlpStreamExtensions
{
    public static KeccakOrRlp ToKeccakOrRlp(this scoped RlpStream stream)
    {
        return KeccakOrRlp.FromSpan(stream.Data);
    }
}
