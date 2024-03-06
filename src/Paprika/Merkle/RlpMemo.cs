using System.Diagnostics;
using Paprika.Crypto;
using Paprika.RLP;

namespace Paprika.Merkle;

public readonly ref struct RlpMemo
{
    private readonly Span<byte> _buffer;

    public const int Size = NibbleSet.NibbleCount * Keccak.Size;

    public RlpMemo(Span<byte> buffer)
    {
        Debug.Assert(buffer.Length == Size);

        _buffer = buffer;
    }

    public ReadOnlySpan<byte> Raw => _buffer;

    public void SetRaw(ReadOnlySpan<byte> keccak, byte nibble)
    {
        Debug.Assert(keccak.Length == Keccak.Size);
        keccak.CopyTo(GetAtNibble(nibble));
    }

    public void Set(in KeccakOrRlp keccakOrRlp, byte nibble)
    {
        var span = GetAtNibble(nibble);

        if (keccakOrRlp.DataType == KeccakOrRlp.Type.Keccak)
        {
            keccakOrRlp.Span.CopyTo(span);
        }
        else
        {
            // on rlp, memoize none
            span.Clear();
        }
    }

    public void Clear(byte nibble)
    {
        GetAtNibble(nibble).Clear();
    }

    public bool TryGetKeccak(byte nibble, out ReadOnlySpan<byte> keccak)
    {
        var span = GetAtNibble(nibble);

        if (span.IndexOfAnyExcept((byte)0) >= 0)
        {
            keccak = span;
            return true;
        }

        keccak = default;
        return false;
    }

    private Span<byte> GetAtNibble(byte nibble) => _buffer.Slice(nibble * Keccak.Size, Keccak.Size);
}
