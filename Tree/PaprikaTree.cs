using System.Buffers;
using System.Buffers.Binary;

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

    // extension, identified by no flag set
    private const byte ExtensionType = 0b0000_0000;

    // branch
    private const byte BranchType = 0b1000_0000;
    private const byte BranchChildCountMask = 0b0000_1111;
    private const int BranchMinChildCount = 2;

    // nibbles
    private const int NibbleCardinality = 16;
    private const int NibbleBitSize = 4;
    private const int NibbleMask = (1 << NibbleBitSize) - 1;

    private readonly IDb _db;

    private long _root = Null;

    public PaprikaTree(IDb db)
    {
        _db = db;
    }

    public void Set(ReadOnlySpan<byte> key, ReadOnlySpan<byte> value) => _root = Set(_db, false, _root, 0, key, value);

    private static long Set(IDb db, bool isUpdatable, long current, int nibble, ReadOnlySpan<byte> key,
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
            else
            {
                // split to branch, on the next nibble

                // build the key for the nested nibble, this one exist and 1lvl deeper is needed
                builtKey = BuildKey(nibble + 1, key);
                var @new = WriteLeaf(db, builtKey, value);

                // build the key for the existing one,
                builtKey = TrimKeyTo(nibble + 1, leaf.Slice(0, keyLength));
                var @old = WriteLeaf(db, builtKey, leaf.Slice(keyLength));

                // current node will be overwritten, reporting to db as freed to gather statistics
                db.Free(current);

                // 1. write branch
                return WriteBranch(db, new NibbleEntry(oldNibble, @old), new NibbleEntry(newNibble, @new));
            }
        }

        if ((first & BranchType) == BranchType)
        {
            var count = (first & BranchChildCountMask) + BranchMinChildCount;
            var newNibble = GetNibble(nibble, key[nibble / 2]);
            var found = false;

            Span<byte> updated = stackalloc byte[node.Length + NibbleEntry.Size];
            int copied = 0;

            for (var i = 0; i < count; i++)
            {
                var rawEntry = node.Slice(PrefixLength + i * NibbleEntry.Size);
                var (branchNibble, branchNode) = NibbleEntry.Read(rawEntry);
                var updateTo = updated.Slice(PrefixLength + copied * NibbleEntry.Size);

                if (branchNibble != newNibble)
                {
                    rawEntry.CopyTo(updateTo);
                    copied++;
                }
                else
                {
                    var @new = Set(db, isUpdatable, branchNode, nibble + 1, key, value);

                    new NibbleEntry(newNibble, @new).Write(updateTo);
                    copied++;
                    found = true;

                    if (@new == branchNode)
                    {
                        // not changed
                        return current;
                    }
                }
            }

            // not found, must add
            // 1. add new leaf
            // 2. add it to the branch
            if (!found)
            {
                var builtKey = BuildKey(nibble + 1, key);
                var @new = WriteLeaf(db, builtKey, value);

                var updateTo = updated.Slice(PrefixLength + copied * NibbleEntry.Size);
                new NibbleEntry(newNibble, @new).Write(updateTo);
                copied++;
            }

            updated[0] = (byte)(BranchType | (copied - BranchMinChildCount));

            var toWrite = updated.Slice(0, PrefixLength + copied * NibbleEntry.Size);

            var shouldTryUpdate = ShouldTryUpdate(isUpdatable, nibble) && count == NibbleCardinality;

            if (shouldTryUpdate)
            {
                if (db.TryGetUpdatable(current, out var updatable))
                {
                    // reuse updatable node
                    toWrite.CopyTo(updatable);
                    return current;
                }

                // current node will be overwritten, reporting to db as freed to gather statistics
                db.Free(current);

                return db.Write(toWrite);
            }

            // current node will be overwritten, reporting to db as freed to gather statistics
            db.Free(current);
            return db.Write(toWrite);
        }

        throw new Exception("Type not handled!");
    }

    private static bool ShouldTryUpdate(bool isUpdatable, int nibble) => isUpdatable & nibble < 5;

    // module nibble get fast
    private static byte GetNibble(int nibble, int value) =>
        (byte)((value >> ((nibble & 1) * NibbleBitSize)) & NibbleMask);

    /// <summary>
    /// Build the key.
    /// For even nibbles, it will start with the given nibble.
    /// For odd nibble, includes the previous one as well.
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

    private static long WriteBranch(IDb db, NibbleEntry nibble1, NibbleEntry nibble2)
    {
        Span<byte> branch = stackalloc byte[PrefixLength + 2 * NibbleEntry.Size];

        branch[0] = BranchType | (2 - BranchMinChildCount);

        nibble1.Write(branch.Slice(PrefixLength));
        nibble2.Write(branch.Slice(PrefixLength + NibbleEntry.Size));

        return db.Write(branch);
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
                else
                {
                    value = default;
                    return false;
                }
            }

            if ((first & BranchType) == BranchType)
            {
                var count = (first & BranchChildCountMask) + BranchMinChildCount;
                var newNibble = GetNibble(nibble, key[nibble / 2]);
                var found = false;

                for (var i = 0; i < count; i++)
                {
                    var (branchNibble, branchNode) = NibbleEntry.Read(node.Slice(PrefixLength + i * NibbleEntry.Size));
                    if (branchNibble == newNibble)
                    {
                        // found descendant, set it and follow
                        current = branchNode;
                        found = true;
                        break;
                    }
                }

                if (found)
                {
                    // continue the outer loop
                    continue;
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

    struct NibbleEntry
    {
        public const int Size = 8;
        private const int Shift = 60;
        private const long NodeMask = 0x0FFF_FFFF_FFFF_FFFF;

        public NibbleEntry(byte nibble, long node)
        {
            Encoded = node | ((long)nibble) << Shift;
        }

        public long Encoded;

        public void Write(Span<byte> destination) => BinaryPrimitives.WriteInt64LittleEndian(destination, Encoded);

        public static (byte nibble, long node) Read(ReadOnlySpan<byte> source)
        {
            var value = BinaryPrimitives.ReadInt64LittleEndian(source);
            var node = value & NodeMask;
            var nibble = (byte)((value >> Shift) & NibbleMask);
            return (nibble, node);
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
        }

        void IBatch.Set(ReadOnlySpan<byte> key, ReadOnlySpan<byte> value)
        {
            _root = PaprikaTree.Set(_db, true, _root, 0, key, value);
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