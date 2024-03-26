using System.Diagnostics;
using Paprika.Crypto;
using Paprika.RLP;

namespace Paprika.Merkle;

public readonly ref struct RlpMemo
{
    public static readonly byte[] Empty = new byte[Size];

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

    public static RlpMemo Decompress(scoped in ReadOnlySpan<byte> leftover, NibbleSet.Readonly children,
        scoped in Span<byte> workingSet)
    {
        var span = workingSet[..Size];

        if (leftover.IsEmpty)
        {
            // no RLP cached yet
            span.Clear();
            return new RlpMemo(span);
        }

        if (leftover.Length == Size)
        {
            leftover.CopyTo(span);
            return new RlpMemo(span);
        }

        // It's neither empty nor full. It must be the compressed form, prepare setup first
        span.Clear();
        var memo = new RlpMemo(span);

        // Extract empty bits if any
        NibbleSet.Readonly empty;

        if (leftover.Length % Keccak.Size == NibbleSet.MaxByteSize)
        {
            var bits = leftover[^NibbleSet.MaxByteSize..];
            NibbleSet.Readonly.ReadFrom(bits, out empty);
        }
        else
        {
            empty = default;
        }

        var at = 0;
        for (byte i = 0; i < NibbleSet.NibbleCount; i++)
        {
            if (children[i] && empty[i] == false)
            {
                var keccak = leftover.Slice(at * Keccak.Size, Keccak.Size);
                at++;

                memo.SetRaw(keccak, i);
            }
        }

        return memo;
    }

    public static int Compress(scoped in ReadOnlySpan<byte> memoizedRlp, NibbleSet.Readonly children, scoped in Span<byte> writeTo)
    {
        var memo = new RlpMemo(ComputeMerkleBehavior.MakeRlpWritable(memoizedRlp));
        var at = 0;

        var empty = new NibbleSet();

        for (byte i = 0; i < NibbleSet.NibbleCount; i++)
        {
            if (children[i])
            {
                if (memo.TryGetKeccak(i, out var keccak))
                {
                    var dest = writeTo.Slice(at * Keccak.Size, Keccak.Size);
                    at++;
                    keccak.CopyTo(dest);
                }
                else
                {
                    empty[i] = true;
                }
            }
        }

        Debug.Assert(at != 16 || empty.SetCount == 0, "If at = 16, empty should be empty");

        if (empty.SetCount == children.SetCount)
        {
            // None of children has their Keccak memoized. Instead of reporting it, return nothing written.
            return 0;
        }

        if (empty.SetCount > 0)
        {
            var dest = writeTo.Slice(at * Keccak.Size, NibbleSet.MaxByteSize);
            new NibbleSet.Readonly(empty).WriteToWithLeftover(dest);
            return at * Keccak.Size + NibbleSet.MaxByteSize;
        }

        // Return only children that were written
        return at * Keccak.Size;
    }
}
