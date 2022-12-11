using System.Buffers;
using System.Runtime.CompilerServices;

namespace Tree;

/// <summary>
/// Patricia tree.
/// </summary>
/// <remarks>
/// Nibble path encoding
///
/// This is done in a way to optimize slicing.
/// To get a path shorter by 1, that will be of
/// - odd length, nothing is done, as previous nibble matches the path.
/// - even length, slice by 1, the previous nibble was odd.
/// This allows for efficient operations.
///
/// For comments, B - branch, E - extension, L1, L2...L16 - leaves.
/// </remarks>
public class PaprikaTree
{
    private const long Null = 0;
    private const int KeccakLength = 32;
    private const int KeyLenght = KeccakLength;
    private const int NibblePerByte = 2;
    private const int NibbleCount = KeyLenght * NibblePerByte;
    private const int PooledMinSize = 64;
    private const int PrefixLength = 1;

    // leaf
    private const byte LeafType = 0b0100_0000;

    // extension: [path....][long]
    private const byte ExtensionType = 0b0000_0000;
    private const byte ExtensionNibbleLength = 0b0011_1111; // to 63 nibbles, if KeyLength > 32, needs to be rethough

    // branch
    private const byte BranchType = 0b1000_0000;
    private const byte BranchChildCountMask = 0b0000_1111;
    
    // nibbles
    private const int NibbleBitSize = 4;
    private const int NibbleMask = (1 << NibbleBitSize) - 1;

    private readonly IDb _db;

    private long _root = Null;

    public PaprikaTree(IDb db)
    {
        _db = db;
    }

    public void Set(ReadOnlySpan<byte> key, ReadOnlySpan<byte> value) => _root = Set(_db, _root, 0, key, value);

    private static long Set(IDb db, long current, int nibble, ReadOnlySpan<byte> key,
        ReadOnlySpan<byte> value)
    {
        if (current == Null)
        {
            return WriteLeaf(db, key, value);
        }

        var node = db.Read(current);

        ref readonly var first = ref node[0];

        if ((first & LeafType) == LeafType)
        {
            var leaf = node.Slice(PrefixLength);
            var builtKey = BuildKey(nibble, key);
            var keyLength = builtKey.Length;
            var sameKey = leaf.StartsWith(builtKey);

            if (sameKey)
            {
                // current node will be overwritten, reporting to db as freed to gather statistics
                db.Free(current);

                // update in place, making it walk up the tree
                return WriteLeaf(db, builtKey, value);
            }

            // calculate shift to nibble
            var shift = NibbleBitSize * (nibble & 1);
            var newNibble = (byte)((builtKey[0] >> shift) & NibbleMask);
            var oldNibble = (byte)((leaf[0] >> shift) & NibbleMask);

            if (newNibble == oldNibble)
            {
                throw new Exception("Extension case. It is not handled now.");
            }

            // split to branch, on the next nibble
            var fromNibble = nibble + 1;

            // build the key for the nested nibble, this one exist and 1lvl deeper is needed
            builtKey = BuildKey(fromNibble, key);
            var @new = WriteLeaf(db, builtKey, value);

            // build the key for the existing one,
            builtKey = TrimKeyTo(fromNibble, leaf.Slice(0, keyLength));
            var @old = WriteLeaf(db, builtKey, leaf.Slice(keyLength));

            // current node will be overwritten, reporting to db as freed to gather statistics
            db.Free(current);

            Branch branch = default;
            unsafe
            {
                branch.Branches[oldNibble] = @old;
                branch.Branches[newNibble] = @new;
            }

            // 1. write branch
            return branch.WriteTo(db);
        }

        if ((first & BranchType) == BranchType)
        {
            var newNibble = GetNibble(nibble, key[nibble / 2]);
            var branch = Branch.Read(node);

            unsafe
            {
                ref var branchNode = ref branch.Branches[newNibble];
                if (branchNode != Null)
                {
                    var @new = Set(db, branchNode, nibble + 1, key, value);
                    if (@new == branchNode)
                    {
                        // nothing to update in the branch
                        return current;
                    }

                    // override with the new value
                    branchNode = @new;
                }
                else
                {
                    // not exist yet
                    var builtKey = BuildKey(nibble + 1, key);
                    branchNode = WriteLeaf(db, builtKey, value);
                }
            }

            Span<byte> written = branch.WriteTo(stackalloc byte[Branch.MaxDestinationSize]);

            if (db.TryGetUpdatable(current, out var updatable) && written.TryCopyTo(updatable))
            {
                // the current was updatable and was written to, return.
                return current;
            }

            // current node will be overwritten, reporting to db as freed to gather statistics
            db.Free(current);

            return db.Write(written);
        }

        throw new Exception("Type not handled!");
    }

    // module nibble get fast
    private static byte GetNibble(int nibble, byte value) =>
        (byte)((value >> ((nibble & 1) * NibbleBitSize)) & NibbleMask);

    /// <summary>
    /// Build the key.
    /// For even nibbles, it will start with the given nibble.
    /// For odd nibble, includes the previous one as well to be byte aligned.
    /// </summary>
    private static ReadOnlySpan<byte> BuildKey(int nibble, ReadOnlySpan<byte> original)
    {
        return original.Slice(nibble / 2);
    }

    /// <summary>
    /// Trims the existing written key by one nibble in front. This is done in an efficient manner.
    /// </summary>
    /// <param name="nextNibble">The next nibble of the key, absolute, not relative.</param>
    /// <param name="original">The original key that was written to a leaf.</param>
    /// <returns></returns>
    private static ReadOnlySpan<byte> TrimKeyTo(int nextNibble, ReadOnlySpan<byte> original)
    {
        // even nibble, remove 1
        if (nextNibble % 2 == 0)
        {
            // this was odd key, just slice, removing odd nibble in front
            return original.Slice(1);
        }

        // odd, leave as is
        return original;
    }

    private static long WriteLeaf(IDb db, ReadOnlySpan<byte> key, ReadOnlySpan<byte> value)
    {
        var length = PrefixLength + key.Length + value.Length;

        byte[]? array = null;
        Span<byte> destination = length <= PooledMinSize
            ? stackalloc byte[length]
            : array = ArrayPool<byte>.Shared.Rent(length);

        try
        {
            destination[0] = LeafType;
            key.CopyTo(destination.Slice(PrefixLength));
            value.CopyTo(destination.Slice(key.Length + PrefixLength));
            return db.Write(destination.Slice(0, length));
        }
        finally
        {
            if (array != null)
            {
                ArrayPool<byte>.Shared.Return(array);
            }
        }
    }

    public bool TryGet(ReadOnlySpan<byte> key, out ReadOnlySpan<byte> value) => TryGet(_db, _root, key, out value);

    private static bool TryGet(IDb db, long root, ReadOnlySpan<byte> key, out ReadOnlySpan<byte> value)
    {
        if (root == Null)
        {
            value = default;
            return false;
        }

        var current = root;

        for (int nibble = 0; nibble < NibbleCount; nibble++)
        {
            var node = db.Read(current);
            ref readonly var first = ref node[0];

            if ((first & LeafType) == LeafType)
            {
                var leaf = node.Slice(PrefixLength);

                var builtKey = BuildKey(nibble, key);
                var sameKey = leaf.StartsWith(builtKey);

                if (sameKey)
                {
                    value = node.Slice(PrefixLength + builtKey.Length);
                    return true;
                }

                value = default;
                return false;
            }

            if ((first & BranchType) == BranchType)
            {
                var newNibble = GetNibble(nibble, key[nibble / 2]);

                var branch = Branch.Read(node);
                unsafe
                {
                    var jump = branch.Branches[newNibble];
                    if (jump != Null)
                    {
                        current = jump;
                        continue;
                    }
                }

                value = default;
                return false;
            }

            throw new Exception("Type not handled!");
        }

        value = default;
        return false;
    }

    public IBatch Begin() => new Batch(this);

    // enum NodeType : byte
    // {
    //     Branch,
    //     Extension,
    //     Leaf
    // }

    struct Branch
    {
        public const int MaxDestinationSize = BranchCount * EntrySize + PrefixLength;
        private const int BranchCount = 16;
        private const int EntrySize = 8;
        private const int Shift = 60;
        private const long NodeMask = 0x0FFF_FFFF_FFFF_FFFF;
        private const int BranchMinChildCount = 2;

        public unsafe fixed long Branches[BranchCount];

        public static unsafe Branch Read(in ReadOnlySpan<byte> source)
        {
            Branch result = default; // zero undefined

            ref var b = ref Unsafe.AsRef(in source[0]);
            var count = (b & BranchChildCountMask) + BranchMinChildCount;

            // consume first
            b = ref Unsafe.Add(ref b, PrefixLength);

            for (var i = 0; i < count; i++)
            {
                var value = Unsafe.ReadUnaligned<long>(ref Unsafe.Add(ref b, i * EntrySize));
                var node = value & NodeMask;
                var nibble = (byte)((value >> Shift) & NibbleMask);

                result.Branches[nibble] = node;
            }

            return result;
        }

        public long WriteTo(IDb db)
        {
            Span<byte> destination = stackalloc byte[MaxDestinationSize];
            return db.Write(WriteTo(destination));
        }

        public Span<byte> WriteTo(Span<byte> destination)
        {
            ref var b = ref Unsafe.AsRef(in destination[0]);

            int count = 0;
            for (long i = 0; i < BranchCount; i++)
            {
                unsafe
                {
                    if (Branches[i] != Null)
                    {
                        long value = Branches[i] | (i << Shift);
                        Unsafe.WriteUnaligned(ref Unsafe.Add(ref b, PrefixLength + count * EntrySize), value);
                        count++;
                    }
                }
            }

            b = (byte)(BranchType | (byte)(count - BranchMinChildCount));

            return destination.Slice(0, PrefixLength + EntrySize * count);
        }
    }

    class Batch : IBatch
    {
        private readonly PaprikaTree _parent;
        private readonly IDb _db;
        private long _root;

        public Batch(PaprikaTree parent)
        {
            _parent = parent;
            _db = parent._db;
            _root = parent._root;
            _db.StartUpgradableRegion();
        }

        void IBatch.Set(ReadOnlySpan<byte> key, ReadOnlySpan<byte> value)
        {
            _root = PaprikaTree.Set(_db, _root, 0, key, value);
        }

        bool IBatch.TryGet(ReadOnlySpan<byte> key, out ReadOnlySpan<byte> value) =>
            PaprikaTree.TryGet(_db, _root, key, out value);

        public void Commit()
        {
            _db.Seal();
            _parent._root = _root;
        }
    }
}