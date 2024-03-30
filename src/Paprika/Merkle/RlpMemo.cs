using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Runtime.Intrinsics;
using Paprika.Crypto;
using Paprika.Data;
using Paprika.RLP;

namespace Paprika.Merkle;

public readonly ref struct RlpMemo
{
    public static readonly byte[] Empty = new byte[Size];

    private readonly ref byte data;

    public const int Size = NibbleSet.NibbleCount * Keccak.Size;

    public RlpMemo(Span<byte> buffer)
    {
        Debug.Assert(buffer.Length == Size);

        data = ref MemoryMarshal.GetReference(buffer);
    }

    public ReadOnlySpan<byte> Raw => MemoryMarshal.CreateReadOnlySpan(ref data, Size);

    public void SetRaw(ReadOnlySpan<byte> keccak, byte nibble)
    {
        Debug.Assert(keccak.Length == Keccak.Size);
        keccak.CopyTo(MemoryMarshal.CreateSpan(ref GetAtNibble(nibble), Keccak.Size));
    }

    private void SetUnaligned(in Keccak keccak, byte nibble)
    {
        Unsafe.WriteUnaligned(ref GetAtNibble(nibble), keccak);
    }

    private void SetUnaligned(in byte from, byte nibble)
    {
        Unsafe.WriteUnaligned(ref GetAtNibble(nibble), Unsafe.ReadUnaligned<Keccak>(in from));
    }

    private Keccak GetUnaligned(byte nibble)
    {
        return Unsafe.ReadUnaligned<Keccak>(ref GetAtNibble(nibble));
    }


    public void Set(in KeccakOrRlp keccakOrRlp, byte nibble)
    {
        if (keccakOrRlp.DataType == KeccakOrRlp.Type.Keccak)
        {
            SetUnaligned(keccakOrRlp.Unsafe, nibble);
        }
        else
        {
            Clear(nibble);
        }
    }

    public void Clear(byte nibble)
    {
        Unsafe.WriteUnaligned(ref GetAtNibble(nibble), default(Keccak));
    }

    public bool TryGetKeccak(byte nibble, out Keccak keccak)
    {
        keccak = GetUnaligned(nibble);
        return keccak != default;
    }

    private ref byte GetAtNibble(byte nibble) => ref Unsafe.Add(ref data, nibble * Keccak.Size);

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
        ref var source = ref MemoryMarshal.GetReference(leftover);

        for (byte i = 0; i < NibbleSet.NibbleCount; i++)
        {
            if (children[i] && empty[i] == false)
            {
                memo.SetUnaligned(Unsafe.Add(ref source, at * Keccak.Size), i);
                at++;
            }
        }

        return memo;
    }

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

        ref var dest = ref MemoryMarshal.GetReference(writeTo);

        for (byte i = 0; i < NibbleSet.NibbleCount; i++)
        {
            if (children[i])
            {
                if (memo.TryGetKeccak(i, out var keccak))
                {
                    Unsafe.WriteUnaligned(ref Unsafe.Add(ref dest, at * Keccak.Size), keccak);
                    at++;
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
            var emptyDest = writeTo.Slice(at * Keccak.Size, NibbleSet.MaxByteSize);
            new NibbleSet.Readonly(empty).WriteToWithLeftover(emptyDest);
            return at * Keccak.Size + NibbleSet.MaxByteSize;
        }

        // Return only children that were written
        return at * Keccak.Size;
    }
}
