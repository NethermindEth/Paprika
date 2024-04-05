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

    public readonly Keccak Keccak;
    public readonly int Length;

    public readonly Type DataType => Length == KeccakLength ? Type.Keccak : Type.Rlp;

    public Span<byte> Span => Keccak.BytesAsSpan[..Length];

    private KeccakOrRlp(in Keccak keccak)
    {
        Keccak = keccak;
        Length = KeccakLength;
    }

    public static implicit operator KeccakOrRlp(in Keccak keccak) => new(in keccak);

    [SkipLocalsInit]
    public static void FromSpan(scoped ReadOnlySpan<byte> data, out KeccakOrRlp keccak)
    {
        Unsafe.SkipInit(out keccak);

        if (data.Length < KeccakLength)
        {
            // Zero out keccak as we might not use all of it
            Unsafe.AsRef(in keccak.Keccak) = default;
            // Set length to the data length
            Unsafe.AsRef(in keccak.Length) = (byte)data.Length;
            // Copy data to keccak space
            data.CopyTo(keccak.Keccak.BytesAsSpan);
        }
        else
        {
            // Compute hash directly to keccak
            KeccakHash.ComputeHash(data, keccak.Keccak.BytesAsSpan);
            // Set length to KeccakLength
            Unsafe.AsRef(in keccak.Length) = (byte)KeccakLength;
        }
    }

    public override string ToString() => DataType == Type.Keccak
        ? $"Keccak: {Keccak.ToString()}"
        : $"RLP: {SpanExtensions.ToHexString(Span, true)}";
}

public static class RlpStreamExtensions
{
    public static void ToKeccakOrRlp(this ref RlpStream stream, out KeccakOrRlp keccak)
    {
        KeccakOrRlp.FromSpan(stream.Data, out keccak);
    }
}
