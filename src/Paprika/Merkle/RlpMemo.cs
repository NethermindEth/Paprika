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
    public static readonly byte[] FullyEmpty = new byte[Size];

    public static readonly byte[] Empty = [];

    private readonly Span<byte> _buffer;

    public const int Size = NibbleSet.NibbleCount * Keccak.Size;

    public RlpMemo(Span<byte> buffer)
    {
        _buffer = buffer;
    }

    public RlpMemo(int count)
    {
        _buffer = new byte[count * Keccak.Size];
    }

    public ReadOnlySpan<byte> Raw => _buffer;

    public int Length => _buffer.Length;

    public void SetRaw(ReadOnlySpan<byte> keccak, byte nibble, NibbleSet.Readonly children)
    {
        var span = GetAtNibble(nibble, children);
        Debug.Assert(span != null);
        Debug.Assert(_buffer.Length == (children.SetCount * Keccak.Size));

        keccak.CopyTo(span);
    }

    public void Set(in KeccakOrRlp keccakOrRlp, byte nibble, NibbleSet.Readonly children)
    {
        var span = GetAtNibble(nibble, children);
        Debug.Assert(span != null);
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

        if (span != null)
        {
            span.Clear();
        }
    }

    public bool TryGetKeccak(byte nibble, out ReadOnlySpan<byte> keccak, NibbleSet.Readonly children)
    {
        var span = GetAtNibble(nibble, children);
        Debug.Assert(_buffer.Length == 0 || _buffer.Length == (children.SetCount * Keccak.Size));

        if (span != null && span.IndexOfAnyExcept((byte)0) >= 0)
        {
            keccak = span;
            return true;
        }

        keccak = default;
        return false;
    }

    public bool Exists(byte nibble, NibbleSet.Readonly children)
    {
        var leftChildren = (ushort)((ushort)children & ((1U << (nibble + 1)) - 1));

        // Check if the element exists
        if (_buffer.Length == 0 || (leftChildren & (1U << nibble)) == 0)
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
            return null;
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
            var remainingBytes = (children.SetCount - insertIndex - 1) * Keccak.Size;

            // Copy all the elements after the new element
            if (remainingBytes > 0)
            {
                memo._buffer.Slice(insertOffset, remainingBytes)
                    .CopyTo(span.Slice(insertOffset + Keccak.Size));
            }

            // Copy elements before the new element
            if (insertOffset > 0)
            {
                memo._buffer.Slice(0, insertOffset).CopyTo(span);
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

        // Copy elements after the deleted element
        int remainingBytes = (children.SetCount - deleteIndex) * Keccak.Size;
        if (remainingBytes > 0)
        {
            memo._buffer.Slice(deleteOffset + Keccak.Size, remainingBytes)
                .CopyTo(span.Slice(deleteOffset));
        }

        // Copy elements before the deleted element
        if (deleteOffset > 0)
        {
            memo._buffer.Slice(0, deleteOffset).CopyTo(span);
        }

        return new RlpMemo(span);
    }

    public static RlpMemo Decompress(scoped in ReadOnlySpan<byte> leftover, NibbleSet.Readonly children,
        scoped in Span<byte> workingSet)
    {
        if (_decompressionForbidden)
        {
            ThrowDecompressionForbidden();
        }

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

        // The empty bytes length is anything that is not aligned to the Keccak size
        var emptyBytesLength = leftover.Length % Keccak.Size;
        if (emptyBytesLength > 0)
        {
            var bits = leftover[^emptyBytesLength..];
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

                memo.SetRaw(keccak, i, children);
            }
        }

        return memo;

        [DoesNotReturn]
        [MethodImpl(MethodImplOptions.NoInlining)]
        static void ThrowDecompressionForbidden() => throw new InvalidOperationException("Decompression is forbidden.");
    }

    [SkipLocalsInit]
    public static int Compress(in Key key, scoped in ReadOnlySpan<byte> memoizedRlp, NibbleSet.Readonly children, scoped in Span<byte> writeTo)
    {
        // Optimization, omitting some of the branches to memoize.
        // It omits only these with two children where the cost of the recompute is not big.
        // To prevent an attack of spawning multiple levels of such branches, only even are skipped
        if (children.SetCount == 2 && (key.Path.Length + key.StoragePath.Length) % 2 == 0)
        {
            return 0;
        }

        var memo = new RlpMemo(ComputeMerkleBehavior.MakeRlpWritable(memoizedRlp));
        var at = 0;

        var empty = new NibbleSet();

        for (byte i = 0; i < NibbleSet.NibbleCount; i++)
        {
            if (children[i])
            {
                if (memo.TryGetKeccak(i, out var keccak, children))
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

    /// <summary>
    /// Test only method to forbid decompression.
    /// </summary>
    /// <returns></returns>
    public static NoDecompressionScope NoDecompression() => new();

    private static volatile bool _decompressionForbidden;

    public readonly struct NoDecompressionScope : IDisposable
    {
        public NoDecompressionScope()
        {
            _decompressionForbidden = true;
        }

        public void Dispose()
        {
            _decompressionForbidden = false;
        }
    }
}
