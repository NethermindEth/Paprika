using System.Buffers;
using System.Buffers.Binary;
using Microsoft.Win32.SafeHandles;

namespace Tree;

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
    private const byte BranchChildCountMask = 0b0111_1111;
    private const int BranchMinChildCount = 2;

    // nibbles
    private const int NibbleBitSize = 4;
    private const int NibbleMask = (1 << NibbleBitSize) - 1;

    private readonly IDb _db;

    private long _root = Null;

    public PaprikaTree(IDb db)
    {
        _db = db;
    }

    public void Set(ReadOnlySpan<byte> key, ReadOnlySpan<byte> value)
    {
        Span<long> path = stackalloc long[NibbleCount];
        path[0] = _root;

        int nibble = 0;
        for (; nibble < NibbleCount + 1; nibble++)
        {
            ref var current = ref path[nibble];

            if (current == Null)
            {
                current = WriteLeaf(key, value);
                break;
            }

            var node = _db.Read(current);

            ref readonly var first = ref node[0];

            if ((first & LeafType) == LeafType)
            {
                // ReSharper disable once StackAllocInsideLoop
                Span<byte> destination = stackalloc byte[key.Length];

                var leaf = node.Slice(PrefixLength);
                var builtKey = BuildKey(nibble, key, destination);
                var keyLength = builtKey.Length;
                var sameKey = leaf.StartsWith(builtKey);

                if (sameKey)
                {
                    // update in place, making it walk up the tree
                    current = WriteLeaf(builtKey, value);
                    break;
                }

                // as keys are truncated to the current depth, use GetNibble for even or odd
                var newNibble = GetNibble(nibble & 1, builtKey[0]);
                var oldNibble = GetNibble(nibble & 1, leaf[0]);

                if (newNibble == oldNibble)
                {
                    throw new Exception("Extension case. It is not handled now.");
                }
                else
                {
                    // split to branch, on the next nibble
                    
                    // build the key for the nested nibble, this one exist and 1lvl deeper is needed
                    builtKey = BuildKey(nibble + 1, key, destination);
                    var @new = WriteLeaf(builtKey, value);

                    // build the key for the existing one,
                    builtKey = TrimKeyTo(nibble + 1, leaf.Slice(0, keyLength), destination);
                    var @old = WriteLeaf(builtKey, leaf.Slice(keyLength));

                    // 1. write branch
                    current = WriteBranch(new NibbleEntry(oldNibble, @old), new NibbleEntry(newNibble, @new));
                    break;
                }
            }
            else if ((first & BranchType) == BranchType)
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
                        path[nibble + 1] = branchNode;
                        found = true;
                        break;
                    }
                }

                if (found)
                {
                    // continue the outer loop
                    continue;
                }

                // not found, must add
                // 1. add new leaf
                // 2. add it to the branch

                // 1. add the leaf
                // ReSharper disable once StackAllocInsideLoop
                Span<byte> destination = stackalloc byte[key.Length];
                var builtKey = BuildKey(nibble + 1, key, destination);
                var added = WriteLeaf(builtKey, value);

                current = AddToBranch(node, count, new NibbleEntry(newNibble, added));
                break;
            }
            else
            {
                throw new Exception("Type not handled!");
            }
        }

        // propagate write up now!
        // nodes above can be either extensions or branches
        // for (var i = nibble - 1; i >= 0; i--)
        // {
        //     path[i]
        // }
        
        
        // TODO: propagate write up, with hashes!
        // currently, not needed, but hashes should be recalculated and propagated up!
        // for (var i = nibble; i >= 0; i--)
        // {
        //     build hash up, write a new node
        // }

        _root = path[0];
    }
    
    // module nibble get fast
    private static byte GetNibble(int nibble, int value) =>
        (byte)((value >> ((nibble & 1) * NibbleBitSize)) & NibbleMask);

    private static ReadOnlySpan<byte> BuildKey(int nibble, ReadOnlySpan<byte> original, Span<byte> buildTo)
    {
        if (nibble % 2 == 0)
        {
            return original.Slice(nibble / 2);
        }

        // remember the high part of the odd nibble as a single byte
        buildTo[0] = (byte)((original[nibble / 2] >> NibbleBitSize) & NibbleMask);
        var copy = original.Slice(nibble / 2 + 1);

        // copy from the next
        copy.CopyTo(buildTo.Slice(1));
        return buildTo.Slice(0, copy.Length + 1);
    }
    
    private static ReadOnlySpan<byte> TrimKeyTo(int nextNibble, ReadOnlySpan<byte> original, Span<byte> buildTo)
    {
        if (nextNibble % 2 == 0)
        {
            // this was odd key, just slice, removing odd nibble in front
            original.Slice(1);
        }

        // the key was even, to make it odd, do as in BuildKey
        original.CopyTo(buildTo);
        buildTo[0] = (byte)((original[0] >> NibbleBitSize) & NibbleMask);
        return buildTo.Slice(0, original.Length);
    }

    private long WriteLeaf(ReadOnlySpan<byte> key, ReadOnlySpan<byte> value)
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
            return _db.Write(destination.Slice(0, length));
        }
        finally
        {
            if (array != null)
            {
                ArrayPool<byte>.Shared.Return(array);
            }
        }
    }

    private long WriteBranch(NibbleEntry nibble1, NibbleEntry nibble2)
    {
        Span<byte> branch = stackalloc byte[PrefixLength + 2 * NibbleEntry.Size];

        branch[0] = BranchType | (2 - BranchMinChildCount);

        nibble1.Write(branch.Slice(PrefixLength));
        nibble2.Write(branch.Slice(PrefixLength + NibbleEntry.Size));

        return _db.Write(branch);
    }

    private long AddToBranch(ReadOnlySpan<byte> currentBranch, int currentCount, NibbleEntry toAdd)
    {
        Span<byte> branch = stackalloc byte[PrefixLength + (currentCount + 1) * NibbleEntry.Size];

        currentBranch.CopyTo(branch);

        branch[0] = (byte)(BranchType | (currentCount + 1 - BranchMinChildCount));

        toAdd.Write(branch.Slice(PrefixLength + NibbleEntry.Size * currentCount));

        return _db.Write(branch);
    }

    public bool TryGet(ReadOnlySpan<byte> key, out ReadOnlySpan<byte> value)
    {
        if (_root == Null)
        {
            value = default;
            return false;
        }

        var current = _root;

        for (int nibble = 0; nibble < NibbleCount; nibble++)
        {
            var node = _db.Read(current);
            ref readonly var first = ref node[0];

            if ((first & LeafType) == LeafType)
            {
                var leaf = node.Slice(PrefixLength);
                Span<byte> destination = stackalloc byte[key.Length];
                
                var builtKey = BuildKey(nibble, key, destination);
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
            else if ((first & BranchType) == BranchType)
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
            else
            {
                throw new Exception("Type not handled!");
            }
        }

        value = default;
        return false;
    }

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
}