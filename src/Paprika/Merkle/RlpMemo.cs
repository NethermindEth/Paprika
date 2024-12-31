using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Numerics;
using System.Runtime.CompilerServices;
using Paprika.Crypto;
using Paprika.Data;
using Paprika.RLP;

namespace Paprika.Merkle;

public readonly ref struct RlpMemo
{
    public static readonly byte[] Empty = [];

    private readonly Span<byte> _buffer;

    public const int MaxSize = NibbleSet.NibbleCount * Keccak.Size;

    public RlpMemo(Span<byte> buffer)
    {
        _buffer = buffer;
    }

    public ReadOnlySpan<byte> Raw => _buffer;

    public int Length => _buffer.Length;

    public void SetRaw(ReadOnlySpan<byte> keccak, byte nibble, NibbleSet.Readonly children)
    {
        var span = GetAtNibble(nibble, children);
        Debug.Assert(!span.IsEmpty);
        Debug.Assert(_buffer.Length == (children.SetCount * Keccak.Size));

        keccak.CopyTo(span);
    }

    public void Set(in KeccakOrRlp keccakOrRlp, byte nibble, NibbleSet.Readonly children)
    {
        var span = GetAtNibble(nibble, children);
        Debug.Assert(!span.IsEmpty);
        Debug.Assert(_buffer.Length == (children.SetCount * Keccak.Size));

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

    public void Clear(byte nibble, NibbleSet.Readonly children)
    {
        var span = GetAtNibble(nibble, children);
        Debug.Assert(_buffer.Length == 0 || _buffer.Length == (children.SetCount * Keccak.Size));

        if (!span.IsEmpty)
        {
            span.Clear();
        }
    }

    public bool TryGetKeccak(byte nibble, out ReadOnlySpan<byte> keccak, NibbleSet.Readonly children)
    {
        var span = GetAtNibble(nibble, children);
        Debug.Assert(_buffer.Length == 0 || _buffer.Length == (children.SetCount * Keccak.Size));

        if (span.IndexOfAnyExcept((byte)0) >= 0)
        {
            keccak = span;
            return true;
        }

        keccak = default;
        return false;
    }

    public bool Exists(byte nibble, NibbleSet.Readonly children)
    {
        // Check if the element exists
        if (_buffer.Length == 0 || ((ushort)children & (1U << nibble)) == 0)
        {
            return false;
        }

        return true;
    }

    private Span<byte> GetAtNibble(byte nibble, NibbleSet.Readonly children)
    {
        var leftChildren = (ushort)((ushort)children & ((1U << (nibble + 1)) - 1));

        // Check if the element exists
        if (_buffer.Length == 0 || (leftChildren & (1U << nibble)) == 0)
        {
            return [];
        }

        var index = BitOperations.PopCount(leftChildren) - 1;
        return _buffer.Slice(index * Keccak.Size, Keccak.Size);
    }

    public static RlpMemo Copy(ReadOnlySpan<byte> from, scoped in Span<byte> to)
    {
        var span = to[..from.Length];
        from.CopyTo(span);
        return new RlpMemo(span);
    }

    public static RlpMemo Insert(RlpMemo memo, byte nibble, NibbleSet.Readonly children,
        ReadOnlySpan<byte> keccak, scoped in Span<byte> workingSet)
    {
        // Compute the destination size for copying.
        var size = children.SetCount * Keccak.Size;

        Debug.Assert(workingSet.Length >= size);
        var span = workingSet[..size];

        // Ensure that this element already exists in the list of children
        var leftChildren = (ushort)((ushort)children & ((1U << (nibble + 1)) - 1));
        Debug.Assert((leftChildren & (1U << nibble)) != 0);

        // Find the index of this nibble in the memo
        var insertIndex = BitOperations.PopCount(leftChildren) - 1;
        var insertOffset = insertIndex * Keccak.Size;

        if (memo.Length != 0)
        {
            // Copy all the elements before the new element
            if (insertOffset > 0)
            {
                memo._buffer.Slice(0, insertOffset).CopyTo(span);
            }

            // Copy all the elements after the new element
            var remainingBytes = (children.SetCount - insertIndex - 1) * Keccak.Size;
            if (remainingBytes > 0)
            {
                memo._buffer.Slice(insertOffset, remainingBytes)
                    .CopyTo(span.Slice(insertOffset + Keccak.Size));
            }
        }
        else
        {
            // Insert empty keccak for all the existing children
            span.Clear();
        }

        keccak.CopyTo(span.Slice(insertOffset));

        return new RlpMemo(span);
    }

    public static RlpMemo Delete(RlpMemo memo, byte nibble, NibbleSet.Readonly children,
        scoped in Span<byte> workingSet)
    {
        // Compute the destination size for copying.
        var size = memo.Length - Keccak.Size;
        if (size < 0)
        {
            // Memo is already empty, nothing to delete.
            size = 0;
            return new RlpMemo(workingSet[..size]);
        }

        var span = workingSet[..size];

        // Ensure that this element does not exist in the list of children
        var leftChildren = (ushort)((ushort)children & ((1U << (nibble + 1)) - 1));
        Debug.Assert((leftChildren & (1U << nibble)) == 0);

        // Find the index of this nibble in the memo
        var deleteIndex = BitOperations.PopCount(leftChildren);
        var deleteOffset = deleteIndex * Keccak.Size;

        // Copy all the elements before the deleted element
        if (deleteOffset > 0)
        {
            memo._buffer.Slice(0, deleteOffset).CopyTo(span);
        }

        // Copy all the elements after the deleted element
        var remainingBytes = (children.SetCount - deleteIndex) * Keccak.Size;
        if (remainingBytes > 0)
        {
            memo._buffer.Slice(deleteOffset + Keccak.Size, remainingBytes)
                .CopyTo(span.Slice(deleteOffset));
        }

        return new RlpMemo(span);
    }
}
