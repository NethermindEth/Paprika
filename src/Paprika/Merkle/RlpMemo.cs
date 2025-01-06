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

    public void Set(ReadOnlySpan<byte> keccak, byte nibble)
    {
        var span = GetAtNibble(nibble);
        Debug.Assert(!span.IsEmpty);

        keccak.CopyTo(span);
    }

    public void Clear(byte nibble)
    {
        var span = GetAtNibble(nibble);

        if (!span.IsEmpty)
        {
            span.Clear();
        }
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

    public bool Exists(byte nibble)
    {
        if (_buffer.Length == 0)
        {
            return false;
        }

        GetIndex(out var index);

        return index[nibble];
    }

    private Span<byte> GetAtNibble(byte nibble)
    {
        if (_buffer.Length == 0)
        {
            return [];
        }

        GetIndex(out var index);

        // Check if the element exists
        if (!index[nibble])
        {
            return [];
        }

        var nibbleIndex = index.SetCountBefore(nibble) - 1;
        return _buffer.Slice(nibbleIndex * Keccak.Size, Keccak.Size);
    }

    private void GetIndex(out NibbleSet.Readonly index)
    {
        // Extract the index bits.
        var indexLength = _buffer.Length % Keccak.Size;

        if (indexLength != 0)
        {
            var bits = _buffer[^indexLength..];
            NibbleSet.Readonly.ReadFrom(bits, out index);
        }
        else
        {
            Debug.Assert(_buffer.Length is 0 or MaxSize);

            index = _buffer.IsEmpty ? NibbleSet.Readonly.None : NibbleSet.Readonly.All;
        }
    }

    public static RlpMemo Copy(ReadOnlySpan<byte> from, scoped in Span<byte> to)
    {
        var span = to[..from.Length];
        from.CopyTo(span);
        return new RlpMemo(span);
    }

    public static RlpMemo Insert(RlpMemo memo, byte nibble, ReadOnlySpan<byte> keccak, scoped in Span<byte> workingSet)
    {
        memo.GetIndex(out var index);

        // Ensure that this element doesn't already exist.
        Debug.Assert(!index[nibble]);

        // Compute the destination size for copying.
        var size = (index.SetCount < NibbleSet.NibbleCount - 1)
            ? (index.SetCount + 1) * Keccak.Size + NibbleSet.MaxByteSize
            : MaxSize;
        Debug.Assert(workingSet.Length >= size);

        var span = workingSet[..size];

        // Compute the index of this nibble in the memo
        var insertIndex = index.SetCountBefore(nibble);
        var insertOffset = insertIndex * Keccak.Size;

        // Copy all the elements before the new element
        if (insertOffset > 0)
        {
            memo._buffer[..insertOffset].CopyTo(span);
        }

        // Copy all the elements after the new element (except the index)
        if (memo.Length > insertOffset)
        {
            memo._buffer[insertOffset..^NibbleSet.MaxByteSize].CopyTo(span[(insertOffset + Keccak.Size)..]);
        }

        keccak.CopyTo(span[insertOffset..]);

        // Update the index.
        index = index.Set(nibble);

        if (size != MaxSize)
        {
            index.WriteToWithLeftover(span[^NibbleSet.MaxByteSize..]);
        }

        return new RlpMemo(span);
    }

    public static RlpMemo Delete(RlpMemo memo, byte nibble, scoped in Span<byte> workingSet)
    {
        memo.GetIndex(out var index);

        // Ensure that this element isn't already deleted.
        Debug.Assert(index[nibble]);

        // Compute the destination size for copying.
        var size = (index.SetCount < NibbleSet.NibbleCount)
            ? memo.Length - Keccak.Size
            : memo.Length - Keccak.Size + NibbleSet.MaxByteSize;

        if (size <= NibbleSet.MaxByteSize)
        {
            // Empty RlpMemo after this delete operation.
            size = 0;
            return new RlpMemo(workingSet[..size]);
        }

        var span = workingSet[..size];

        // Compute the index of this nibble in the memo
        var deleteIndex = index.SetCountBefore(nibble) - 1;
        var deleteOffset = deleteIndex * Keccak.Size;

        // Copy all the elements before the deleted element
        if (deleteOffset > 0)
        {
            memo._buffer[..deleteOffset].CopyTo(span);
        }

        // Copy all the elements after the deleted element (except the index)
        if (memo.Length > (deleteOffset + Keccak.Size))
        {
            memo._buffer[(deleteOffset + Keccak.Size)..^NibbleSet.MaxByteSize].CopyTo(span[deleteOffset..]);
        }

        // Update the index.
        index = index.Remove(nibble);
        index.WriteToWithLeftover(span[^NibbleSet.MaxByteSize..]);

        return new RlpMemo(span);
    }
}
