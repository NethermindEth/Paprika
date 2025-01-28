using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
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
        Debug.Assert(!span.IsEmpty, "Attempted to set a value on a non-existent index");
        Debug.Assert(keccak.Length == Keccak.Size && span.Length == Keccak.Size, "Attempted to set incorrect length");

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

        if (!span.IsEmpty && span.IndexOfAnyExcept((byte)0) >= 0)
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

        var index = GetIndex();

        return index[nibble];
    }

    private Span<byte> GetAtNibble(byte nibble)
    {
        if (_buffer.Length == 0)
        {
            return [];
        }

        var index = GetIndex();

        // Check if the element exists
        if (!index[nibble])
        {
            return [];
        }

        var nibbleIndex = index.SetCountBefore(nibble) - 1;
        var dataStartOffset = (_buffer.Length != MaxSize) ? NibbleSet.MaxByteSize : 0;
        return _buffer.Slice(dataStartOffset + nibbleIndex * Keccak.Size, Keccak.Size);
    }

    private NibbleSet.Readonly GetIndex()
    {
        // Extract the index bits.
        var indexLength = _buffer.Length % Keccak.Size;
        NibbleSet.Readonly index;

        if (indexLength != 0)
        {
            Debug.Assert(indexLength == NibbleSet.MaxByteSize, "Unexpected index length");
            index = NibbleSet.Readonly.ReadFrom(_buffer);
        }
        else
        {
            Debug.Assert(_buffer.Length is 0 or MaxSize, "Only empty or full RlpMemo can have no index");
            index = _buffer.IsEmpty ? NibbleSet.Readonly.None : NibbleSet.Readonly.All;
        }

        return index;
    }

    public static RlpMemo Copy(ReadOnlySpan<byte> from, scoped in Span<byte> to)
    {
        var span = to[..from.Length];
        from.CopyTo(span);
        return new RlpMemo(span);
    }

    public static RlpMemo Insert(RlpMemo memo, byte nibble, ReadOnlySpan<byte> keccak, scoped in Span<byte> workingSet)
    {
        var index = memo.GetIndex();

        Debug.Assert(!index[nibble], "Attempted to insert a value into an already existing index");

        // Update the index and then compute the destination size for copying.
        index = index.Set(nibble);
        var size = ComputeRlpMemoSize(index);

        Debug.Assert(size is >= Keccak.Size + NibbleSet.MaxByteSize and <= MaxSize, "Unexpected size during insert");
        Debug.Assert(workingSet.Length >= size, "Insufficient destination length for insertion");

        var span = workingSet[..size];

        // Start offsets for the data in the source (if any) and destination memo.
        var sourceStartOffset = (memo.Length != 0) ? NibbleSet.MaxByteSize : 0;
        var destStartOffset = (size != MaxSize) ? NibbleSet.MaxByteSize : 0;

        // Compute the offset of this nibble in the memos
        var insertIndex = index.SetCountBefore(nibble) - 1;
        var insertOffset = destStartOffset + insertIndex * Keccak.Size;
        var sourceRemainingStartOffset = sourceStartOffset + insertIndex * Keccak.Size;

        // Copy all the elements before the new element
        if (insertOffset > destStartOffset)
        {
            memo._buffer[sourceStartOffset..sourceRemainingStartOffset].CopyTo(span[destStartOffset..]);
        }

        // Copy all the elements after the new element
        if (memo.Length > sourceRemainingStartOffset)
        {
            memo._buffer[sourceRemainingStartOffset..].CopyTo(span[(insertOffset + Keccak.Size)..]);
        }

        keccak.CopyTo(span[insertOffset..]);

        // Insert the new index only if the destination memo is not full.
        if (size != MaxSize)
        {
            index.WriteToWithLeftover(span);
        }

        return new RlpMemo(span);
    }

    public static RlpMemo Delete(RlpMemo memo, byte nibble, scoped in Span<byte> workingSet)
    {
        var index = memo.GetIndex();

        Debug.Assert(index[nibble], "Attempted to delete a non-existing index");

        // Update the index and then compute the destination size for copying.
        index = index.Remove(nibble);
        var size = ComputeRlpMemoSize(index);

        Debug.Assert(size is >= 0 and < MaxSize, "Unexpected size during deletion");
        Debug.Assert(workingSet.Length >= size, "Insufficient destination length for deletion");

        if (size == 0)
        {
            // Return empty RlpMemo
            return new RlpMemo(workingSet[..size]);
        }

        var span = workingSet[..size];

        // Start offsets for the data in the source and destination memo. 
        var sourceStartOffset = (memo.Length != MaxSize) ? NibbleSet.MaxByteSize : 0;
        const int destStartOffset = NibbleSet.MaxByteSize;

        // Compute the offset of this nibble in the memos
        var deleteIndex = index.SetCountBefore(nibble);
        var deleteSourceOffset = sourceStartOffset + deleteIndex * Keccak.Size;
        var deleteDestOffset = destStartOffset + deleteIndex * Keccak.Size;

        // Copy all the elements before the deleted element
        if (deleteSourceOffset > sourceStartOffset)
        {
            memo._buffer[sourceStartOffset..deleteSourceOffset].CopyTo(span[destStartOffset..]);
        }

        // Copy all the elements after the deleted element
        if (memo.Length > deleteSourceOffset + Keccak.Size)
        {
            memo._buffer[(deleteSourceOffset + Keccak.Size)..].CopyTo(span[deleteDestOffset..]);
        }

        // Since the destination memo is neither empty nor full here, it must always contain the index.
        index.WriteToWithLeftover(span);

        return new RlpMemo(span);
    }

    private static int ComputeRlpMemoSize(NibbleSet.Readonly index)
    {
        var size = index.SetCount * Keccak.Size;

        // Add extra space for the index. Empty and full memo doesn't contain the index.
        if (size != 0 && size != MaxSize)
        {
            size += NibbleSet.MaxByteSize;
        }

        return size;
    }
}
