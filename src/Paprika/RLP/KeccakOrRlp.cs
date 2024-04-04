using Paprika.Crypto;
using System.Runtime.CompilerServices;
using SpanExtensions = Paprika.Crypto.SpanExtensions;

namespace Paprika.RLP;

/// <summary>
/// Represents a result of encoding a node in Trie. If it's length is shorter than 32 bytes, then it's an RLP.
/// If it's equal or bigger, then it's its Keccak.
/// </summary>
public readonly struct KeccakOrRlp
{
    private const int KeccakLength = 32;

    public enum Type : byte
    {
        Keccak = 0,
        Rlp = 1,
    }

    private readonly Keccak _keccak;
    private readonly int _length;

    public readonly Type DataType => _length == KeccakLength ? Type.Keccak : Type.Rlp;

    public Span<byte> Span => _keccak.BytesAsSpan[.._length];

    private KeccakOrRlp(in Keccak keccak)
    {
        _keccak = keccak;
        _length = KeccakLength;
    }

    public static implicit operator KeccakOrRlp(in Keccak keccak) => new(in keccak);

    [SkipLocalsInit]
    public static KeccakOrRlp FromSpan(scoped Span<byte> data)
    {
        KeccakOrRlp keccak;
        Unsafe.SkipInit(out keccak);

        if (data.Length < KeccakLength)
        {
            // Zero out keccak as we might not use all of it
            Unsafe.AsRef(in keccak._keccak) = default;
            // Set length to the data length
            Unsafe.AsRef(in keccak._length) = (byte)data.Length;
            // Copy data to keccak space
            data.CopyTo(keccak._keccak.BytesAsSpan);
        }
        else
        {
            // Compute hash directly to keccak
            KeccakHash.ComputeHash(data, keccak._keccak.BytesAsSpan);
            // Set length to KeccakLength
            Unsafe.AsRef(in keccak._length) = (byte)KeccakLength;
        }

        return keccak;
    }

    public override string ToString() => DataType == Type.Keccak
        ? $"Keccak: {_keccak.ToString()}"
        : $"RLP: {SpanExtensions.ToHexString(Span, true)}";
}

public static class RlpStreamExtensions
{
    public static KeccakOrRlp ToKeccakOrRlp(this scoped RlpStream stream)
    {
        return KeccakOrRlp.FromSpan(stream.Data);
    }
}
