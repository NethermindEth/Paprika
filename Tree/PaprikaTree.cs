using System.Diagnostics.Contracts;
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
    private const int ValueLenght = KeccakLength;
    private const int NibblePerByte = 2;
    private const int NibbleCount = KeyLenght * NibblePerByte;
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

    public void Set(ReadOnlySpan<byte> key, ReadOnlySpan<byte> value) =>
        _root = Set(_db, _root, NibblePath.FromKey(key, 0), value);

    private static long Set(IDb db, long current, in NibblePath addedPath,
        in ReadOnlySpan<byte> value)
    {
        if (current == Null)
        {
            return WriteLeaf(db, addedPath, value);
        }

        var node = db.Read(current);

        ref readonly var first = ref node[0];

        if ((first & LeafType) == LeafType)
        {
            ReadLeaf(node.Slice(PrefixLength), out var existingPath);

            var diffAt = addedPath.FindFirstDifferentNibble(existingPath);

            if (diffAt == addedPath.Length)
            {
                // current node will be overwritten, reporting to db as freed to gather statistics
                db.Free(current);

                // update in place, making it walk up the tree
                return WriteLeaf(db, existingPath, value);
            }

            if (diffAt > 0 )
            {
                throw new Exception("Extension case. It is not handled now.");
            }

            // build the key for the nested nibble, this one exist and 1lvl deeper is needed
            var @new = WriteLeaf(db, addedPath.Slice1(), value);

            // build the key for the existing one,
            var @old = WriteLeaf(db, existingPath.Slice1(), node.Slice(PrefixLength + existingPath.RawByteLength));

            // current node will be overwritten, reporting to db as freed to gather statistics
            db.Free(current);

            Branch branch = default;
            unsafe
            {
                branch.Branches[existingPath.FirstNibble] = @old;
                branch.Branches[addedPath.FirstNibble] = @new;
            }

            // 1. write branch
            return WriteToDb(branch, db);
        }

        if ((first & BranchType) == BranchType)
        {
            var newNibble = addedPath.FirstNibble;
            var branch = Branch.Read(node);

            unsafe
            {
                ref var branchNode = ref branch.Branches[newNibble];
                if (branchNode != Null)
                {
                    var @new = Set(db, branchNode, addedPath.Slice1(), value);
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
                    branchNode = WriteLeaf(db, addedPath.Slice1(), value);
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

    private static long WriteToDb(in Branch branch, IDb db)
    {
        Span<byte> destination = stackalloc byte[Branch.MaxDestinationSize];
        return db.Write(branch.WriteTo(destination));
    }

    // module nibble get fast
    private static byte GetNibble(int nibble, byte value) =>
        (byte)((value >> ((nibble & 1) * NibbleBitSize)) & NibbleMask);

    private static long WriteLeaf(IDb db, NibblePath path, ReadOnlySpan<byte> value)
    {
        var length = PrefixLength + path.MaxLength + value.Length;

        Span<byte> destination = stackalloc byte[length];

        destination[0] = LeafType;
        var leftover = path.WriteTo(destination.Slice(1));
        value.CopyTo(leftover);
        return db.Write(destination.Slice(0, length - leftover.Length + value.Length));
    }

    private static ReadOnlySpan<byte> ReadLeaf(ReadOnlySpan<byte> leaf, out NibblePath path)
    {
        var value = NibblePath.ReadFrom(leaf, out path);
        return value.Slice(0, ValueLenght);
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
                value = ReadLeaf(leaf, out var path);

                if (path.Equals(NibblePath.FromKey(key, nibble)))
                {
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

        [Pure]
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
            _root = PaprikaTree.Set(_db, _root, NibblePath.FromKey(key, 0), value);
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