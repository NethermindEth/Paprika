using System.Buffers.Binary;
using System.Diagnostics;
using System.Numerics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Tree.Crypto;

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
public partial class PaprikaTree
{
    private const long Null = 0;

    // [type][rlp or keccak]
    private const int TypePrefixLength = 1;
    private const int KeccakLength = KeccakHash.HASH_SIZE;

    /// <summary>
    /// The total lenght of the the prefix before actual payload of the given node,
    /// includes: type, keccak-or-rlp.
    /// </summary>
    private const int PrefixTotalLength = TypePrefixLength;

    private const int KeyLenght = 32;
    private const int ValueLenght = 32;

    // types
    private const byte TypeMask = 0b1100_0000;
    public const byte LeafType = 0b0100_0000;
    public const byte ExtensionType = 0b0000_0000;
    public const byte BranchType = 0b1000_0000;

    private readonly IDb _db;

    private long _root = Null;

    private KeccakOrRlp _type = KeccakOrRlp.None;
    private readonly byte[] _keccakOrRlp = new byte[KeccakLength];

    public Span<byte> RootKeccak => TrimToType(_keccakOrRlp, _type);

    // the id which the file was flushed to
    private long _lastFlushTo;

    private readonly Store _store;

    public PaprikaTree(IDb db)
    {
        _db = db;
        _store = new Store(db);
    }

    public void Set(ReadOnlySpan<byte> key, ReadOnlySpan<byte> value) =>
        _root = Set(_store, _root, NibblePath.FromKey(key, 0), value);

    private static long Set(IStore db, long current, in NibblePath addedPath,
        in ReadOnlySpan<byte> value)
    {
        if (current == Null)
        {
            return WriteLeaf(db, addedPath, value);
        }

        var node = db.Read(current);

        ref readonly var first = ref node[0];

        if ((first & TypeMask) == LeafType)
        {
            var leafValue = ReadLeaf(node.Slice(PrefixTotalLength), out var existingPath);

            var diffAt = addedPath.FindFirstDifferentNibble(existingPath);

            if (diffAt == addedPath.Length)
            {
                return WriteLeafUpdatable(db, existingPath, value, current);
            }

            if (diffAt > 0)
            {
                // need to create E -> B -> L1, L2
                // 1. extension path will be of length diffAt
                // 2. branch at diffAt index (one forward)
                // 3. leaves
                // do the bottom up

                // 3. leaves first
                var existing = WriteLeaf(db, existingPath.SliceFrom(diffAt + 1), leafValue);
                var added = WriteLeaf(db, addedPath.SliceFrom(diffAt + 1), value);

                // 2. write branch
                Span<byte> branch = stackalloc byte[Branch.GetNeededSize(2)];
                WriteBranch(branch, existingPath.GetAt(diffAt), existing, addedPath.GetAt(diffAt), @added);
                var branchId = db.Write(branch);

                // 3. extension
                var extensionPath = addedPath.SliceTo(diffAt);
                Span<byte> destination = stackalloc byte[Extension.MaxDestinationSize];
                var extension = Extension.WriteTo(extensionPath, branchId, destination);

                return db.TryUpdateOrAdd(current, extension);
            }
            else
            {
                // need to capture values of nibbles as they are overwritten by potential upgradable
                var nibbleExisting = existingPath.FirstNibble;
                var nibbleAdded = addedPath.FirstNibble;

                // build the key for the nested nibble, this one exist and 1lvl deeper is needed
                var @new = WriteLeaf(db, addedPath.SliceFrom(1), value);

                // write existing one, try use updatable
                var oldValue = node.Slice(PrefixTotalLength + existingPath.RawByteLength);
                var @old = WriteLeafUpdatable(db, existingPath.SliceFrom(1), oldValue, current);

                // write branch
                Span<byte> branch = stackalloc byte[Branch.GetNeededSize(2)];
                WriteBranch(branch, nibbleExisting, old, nibbleAdded, @new);
                return db.Write(branch);
            }
        }

        if ((first & TypeMask) == BranchType)
        {
            var newNibble = addedPath.FirstNibble;

            if (Branch.TryFindExisting(node, newNibble, out var found))
            {
                // found so it can be easily overwritten
                var @new = Set(db, found, addedPath.SliceFrom(1), value);
                Span<byte> copy = stackalloc byte[node.Length];
                node.CopyTo(copy);

                Branch.SetWithExistingNibble(copy, newNibble, @new);
                return db.TryUpdateOrAdd(current, copy);
            }

            // not exist yet
            {
                var @new = WriteLeaf(db, addedPath.SliceFrom(1), value);
                // allocate one more
                Span<byte> copy = stackalloc byte[Branch.GetNextNeededSize(node)];
                node.CopyTo(copy);

                Branch.SetNonExistingYet(copy, newNibble, @new);
                return db.TryUpdateOrAdd(current, copy);
            }
        }

        if ((first & TypeMask) == ExtensionType)
        {
            Extension.Read(node, out var extensionPath, out var jumpTo);

            var diffAt = extensionPath.FindFirstDifferentNibble(addedPath);

            // matches the extension
            if (diffAt == extensionPath.Length)
            {
                // matches extension
                var added = Set(db, jumpTo, addedPath.SliceFrom(diffAt), value);
                Span<byte> extension = stackalloc byte[Extension.MaxDestinationSize];
                extension = Extension.WriteTo(extensionPath, added, extension);

                return db.TryUpdateOrAdd(current, extension);
            }

            // build the key for the new value, one level deeper
            var @new = WriteLeaf(db, addedPath.SliceFrom(diffAt + 1), value);

            Span<byte> branch = stackalloc byte[Branch.GetNeededSize(2)];
            WriteBranch(branch, addedPath.GetAt(diffAt), @new, extensionPath.GetAt(diffAt),
                PushExtensionDown(db, extensionPath, jumpTo, diffAt + 1));


            if (diffAt == 0)
            {
                // the branch is the first, no additional extension needed in front, just overwrite the current
                return db.TryUpdateOrAdd(current, branch);
            }
            else
            {
                // the branch is in the middle and it needs to have an extension first
                var branchId = db.Write(branch);

                // extension of at least length 1
                Span<byte> extension = stackalloc byte[Extension.MaxDestinationSize];
                extension = Extension.WriteTo(extensionPath.SliceTo(diffAt), branchId, extension);

                return db.TryUpdateOrAdd(current, extension);
            }
        }

        throw new Exception("Type not handled!");
    }

    private static void WriteBranch(in Span<byte> destination, byte nibble1, long key1, byte nibble2, long key2)
    {
        // clear first 8 bytes, to overlap with prefix
        Unsafe.As<byte, long>(ref destination[0]) = 0;

        destination[0] = BranchType;

        Branch.SetNonExistingYet(destination, nibble1, key1);
        Branch.SetNonExistingYet(destination, nibble2, key2);
    }

    private static long PushExtensionDown(IStore db, NibblePath extensionPath, long jumpTo, int pushDownBy)
    {
        if (extensionPath.Length == pushDownBy)
        {
            return jumpTo;
        }

        Span<byte> extension = stackalloc byte[Extension.MaxDestinationSize];
        extension = Extension.WriteTo(extensionPath.SliceFrom(pushDownBy), jumpTo, extension);
        return db.Write(extension);
    }

    internal static long WriteLeaf(IStore db, NibblePath path, ReadOnlySpan<byte> value)
    {
        var length = PrefixTotalLength + path.MaxLength + value.Length;

        Span<byte> destination = stackalloc byte[length];

        destination[0] = LeafType;
        var leftover = path.WriteTo(destination.Slice(PrefixTotalLength));
        value.CopyTo(leftover);
        return db.Write(destination.Slice(0, length - leftover.Length + value.Length));
    }

    private static long WriteLeafUpdatable(IStore db, NibblePath path, ReadOnlySpan<byte> value, long current)
    {
        var length = PrefixTotalLength + path.MaxLength + value.Length;

        Span<byte> destination = stackalloc byte[length];

        destination[0] = LeafType;
        var leftover = path.WriteTo(destination.Slice(PrefixTotalLength));
        value.CopyTo(leftover);
        var leaf = destination.Slice(0, length - leftover.Length + value.Length);
        return db.TryUpdateOrAdd(current, leaf);
    }

    private static ReadOnlySpan<byte> ReadLeaf(ReadOnlySpan<byte> leaf, out NibblePath path)
    {
        var value = NibblePath.ReadFrom(leaf, out path);
        return value.Slice(0, ValueLenght);
    }

    public bool TryGet(in ReadOnlySpan<byte> key, out ReadOnlySpan<byte> value) => TryGet(_db, _root, key, out value);

    private static bool TryGet(IDb db, long root, in ReadOnlySpan<byte> key, out ReadOnlySpan<byte> value)
    {
        if (root == Null)
        {
            value = default;
            return false;
        }

        var current = root;

        var keyPath = NibblePath.FromKey(key);

        while (keyPath.Length > 0)
        {
            var node = db.Read(current);
            ref readonly var first = ref node[0];

            NibblePath path;

            if ((first & TypeMask) == LeafType)
            {
                var leaf = node.Slice(PrefixTotalLength);
                value = ReadLeaf(leaf, out path);

                if (path.Equals(path))
                {
                    return true;
                }

                value = default;
                return false;
            }

            if ((first & TypeMask) == BranchType)
            {
                if (Branch.TryFindExisting(node, keyPath.FirstNibble, out var jump))
                {
                    keyPath = keyPath.SliceFrom(1);
                    current = jump;
                    continue;
                }

                value = default;
                return false;
            }

            if ((first & TypeMask) == ExtensionType)
            {
                Extension.Read(node, out path, out var jumpTo);
                var diffAt = path.FindFirstDifferentNibble(keyPath);

                // jump only if it consumes the whole path
                if (diffAt == path.Length)
                {
                    keyPath = keyPath.SliceFrom(diffAt);
                    current = jumpTo;
                    continue;
                }

                value = default;
                return false;
            }
        }

        value = default;
        return false;
    }

    public IBatch Begin() => new Batch(this);

    /// <summary>
    /// Layout:
    /// [1byte - type][2bytes - bitmask]
    /// </summary>
    internal static class Branch
    {
        public const int BranchCount = 16;

        private const int EntrySize = Id.Size;

        // Total size needed for writing one nibble.
        private const int TotalNibbleSize = EntrySize + KeccakLength;

        private const byte BranchChildCountMask = 0b0001_1111;

        private const int BitMaskLength = 2;
        private const int BranchPrefixLength = PrefixTotalLength + BitMaskLength;

        private const long IdMask = 0x0FFF_FFFF_FFFF_FFFF;
        private const int KeccakOrRlpShift = 61;
        private const byte KeccakOrRlpMask = 0x3;

        public static int GetNeededSize(int nibbleCount) => BranchPrefixLength + TotalNibbleSize * nibbleCount;

        /// <summary>
        /// Gets the memory needed for the branch with one more nibble.
        /// </summary>
        public static int GetNextNeededSize(in ReadOnlySpan<byte> node) => node.Length + TotalNibbleSize;

        public static bool TryFindExisting(in ReadOnlySpan<byte> source, byte nibble, out long found)
        {
            ref var b = ref Unsafe.AsRef(in source[0]);
            var count = GetCount(b);

            if (count == BranchCount)
            {
                // special case, full branch node, can directly jump as values are always sorted
                found = Unsafe.ReadUnaligned<long>(ref Unsafe.Add(ref b, BranchPrefixLength + nibble * EntrySize)) &
                        IdMask;
                return true;
            }

            var bitmap = ReadBitMap(ref b);

            var bit = 1 << nibble;
            if ((bitmap & bit) == bit)
            {
                // found in bitmap, count set till this moment
                var countNotNull = CountNotNullNibblesBefore(nibble, bitmap);

                found = Unsafe.ReadUnaligned<long>(ref Unsafe.Add(ref b,
                    BranchPrefixLength + countNotNull * EntrySize)) & IdMask;
                return true;
            }

            found = default;
            return false;
        }

        private static ushort ReadBitMap(ref byte start) =>
            Unsafe.ReadUnaligned<ushort>(ref Unsafe.Add(ref start, TypePrefixLength));

        private static void SetBitMap(ref byte start, ushort value) =>
            Unsafe.WriteUnaligned(ref Unsafe.Add(ref start, TypePrefixLength), value);

        private static int CountNotNullNibblesBefore(byte nibble, ushort bitmap)
        {
            // get next bit then subtract 1 to get a mask for this and lower nibbles
            var mask = (1 << nibble) - 1;
            var masked = bitmap & mask;
            return BitOperations.PopCount((uint)masked);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static int GetCount(byte b) => b & BranchChildCountMask;

        public static void SetWithExistingNibble(Span<byte> copy, byte nibble, long @new)
        {
            ref var b = ref Unsafe.AsRef(in copy[0]);
            var count = GetCount(b);
            var skip = count == BranchCount ? nibble : CountNotNullNibblesBefore(nibble, ReadBitMap(ref b));

            Unsafe.WriteUnaligned(ref Unsafe.Add(ref b, BranchPrefixLength + skip * EntrySize), @new);
        }

        public static void SetNonExistingYet(Span<byte> copy, byte newNibble, long @new)
        {
            // not exists, must be 15 or less nibbles
            ref var b = ref Unsafe.AsRef(in copy[0]);

            var count = GetCount(b);
            var bitmap = ReadBitMap(ref b);

            // TODO: optimize spans moves here

            // move keccak region by one to allow write of the nibble
            var oldKeccakRegion = copy.Slice(count * EntrySize + BranchPrefixLength, count * KeccakLength);
            var newKeccakRegion = copy.Slice(count * EntrySize + BranchPrefixLength + EntrySize, count * KeccakLength);
            oldKeccakRegion.CopyTo(newKeccakRegion);

            var nibblesBefore = CountNotNullNibblesBefore(newNibble, bitmap);

            // if not writing as the last, the segment after needs to be moved
            if (nibblesBefore < count)
            {
                var newCount = count + 1;
                var keccakRegion = copy.Slice(newCount * EntrySize + BranchPrefixLength);
                var keccakFrom = nibblesBefore * KeccakLength;
                keccakRegion.Slice(keccakFrom, keccakRegion.Length - keccakFrom - KeccakLength)
                    .CopyTo(keccakRegion.Slice(keccakFrom + KeccakLength));

                var childRegion = copy.Slice(BranchPrefixLength, newCount * EntrySize);
                var childFrom = nibblesBefore * EntrySize;
                childRegion.Slice(childFrom, childRegion.Length - childFrom - EntrySize)
                    .CopyTo(childRegion.Slice(childFrom + EntrySize));
            }

            // write entry
            Unsafe.WriteUnaligned(ref Unsafe.Add(ref b, BranchPrefixLength + nibblesBefore * EntrySize), @new);

            // set metadata
            SetBitMap(ref b, (ushort)(bitmap | (1 << newNibble)));
            b += 1;
        }

        public static KeccakOrRlp GetKeccakOrRlp(ReadOnlySpan<byte> branch, byte nibble, out Span<byte> span)
        {
            ref var b = ref Unsafe.AsRef(in branch[0]);
            var count = GetCount(b);

            var shift = BranchPrefixLength + count * EntrySize;

            if (count == BranchCount)
            {
                span = MemoryMarshal.CreateSpan(ref Unsafe.Add(ref b, shift + nibble * KeccakLength), KeccakLength);

                var found = Unsafe.ReadUnaligned<long>(ref Unsafe.Add(ref b, BranchPrefixLength + nibble * EntrySize));
                return (KeccakOrRlp)((found >> KeccakOrRlpShift) & KeccakOrRlpMask);
            }

            var bitmap = ReadBitMap(ref b);

            var bit = 1 << nibble;
            if ((bitmap & bit) == bit)
            {
                // found in bitmap, count set till this moment
                var countNotNull = CountNotNullNibblesBefore(nibble, bitmap);
                ref var from = ref Unsafe.Add(ref b, shift + countNotNull * KeccakLength);
                span = MemoryMarshal.CreateSpan(ref from, KeccakLength);

                var found = Unsafe.ReadUnaligned<long>(ref Unsafe.Add(ref b,
                    BranchPrefixLength + countNotNull * EntrySize));
                return (KeccakOrRlp)((found >> KeccakOrRlpShift) & KeccakOrRlpMask);
            }

            // should never happen
            span = default;
            return KeccakOrRlp.None;
        }

        public static void SetKeccakOrRlp(ReadOnlySpan<byte> branch, byte nibble, KeccakOrRlp keccakOrRlp)
        {
            ref var b = ref Unsafe.AsRef(in branch[0]);

            var count = GetCount(b);
            var skip = count == BranchCount ? nibble : CountNotNullNibblesBefore(nibble, ReadBitMap(ref b));

            long flag = (long)keccakOrRlp << KeccakOrRlpShift;

            ref var addr = ref Unsafe.Add(ref b, BranchPrefixLength + skip * EntrySize);
            var value = Unsafe.ReadUnaligned<long>(ref addr) & IdMask;
            Unsafe.WriteUnaligned(ref addr, value | flag);
        }

        public static void AssertMemoryDb(in ReadOnlySpan<byte> payload)
        {
            if ((payload[0] & BranchType) == BranchType)
            {
                ref var b = ref Unsafe.AsRef(in payload[0]);
                var count = GetCount(b);

                ref var children = ref Unsafe.Add(ref b, BranchPrefixLength);
                
                for (int i = 0; i < count; i++)
                {
                    var child = Unsafe.ReadUnaligned<long>(ref Unsafe.Add(ref children, i * EntrySize));
                    
                    Debug.Assert(Id.Decode(child).File == MemoryDb.FileNumber, $"{i}th child breaks the rule!");
                }    
            }
        }
    }

    private static class Extension
    {
        private const int ChildIdSize = Id.Size;

        public const int MaxDestinationSize = PrefixTotalLength + 1 + KeyLenght + ChildIdSize;

        public static Span<byte> WriteTo(NibblePath path, long childId, Span<byte> destination)
        {
            destination[0] = ExtensionType;
            var leftover = path.WriteTo(destination.Slice(PrefixTotalLength));
            BinaryPrimitives.WriteInt64LittleEndian(leftover, childId);
            return destination.Slice(0, destination.Length - leftover.Length + ChildIdSize);
        }

        public static void Read(ReadOnlySpan<byte> source, out NibblePath path, out long jumpTo)
        {
            var leftover = NibblePath.ReadFrom(source.Slice(PrefixTotalLength), out path);
            jumpTo = BinaryPrimitives.ReadInt64LittleEndian(leftover);
        }
    }

    class Batch : IBatch
    {
        private readonly PaprikaTree _parent;
        private readonly IDb _db;
        private long _root;

        private readonly long _lastFlushTo;
        private readonly Store _store;

        public Batch(PaprikaTree parent)
        {
            _parent = parent;
            _db = parent._db;
            _root = parent._root;

            _store = parent._store;
            _store.EnsureUpdatable();

            _lastFlushTo = parent._lastFlushTo;
        }

        void IBatch.Set(in ReadOnlySpan<byte> key, in ReadOnlySpan<byte> value)
        {
            _root = PaprikaTree.Set(_store, _root, NibblePath.FromKey(key, 0), value);
        }

        bool IBatch.TryGet(in ReadOnlySpan<byte> key, out ReadOnlySpan<byte> value) =>
            PaprikaTree.TryGet(_db, _root, key, out value);

        public void Commit(CommitOptions options)
        {
            _parent._root = _root;

            switch (options)
            {
                case CommitOptions.RootOnly:
                    // nothing to do
                    break;
                case CommitOptions.RootOnlyWithHash:
                    BuildRootKeccakOrRlp();
                    break;
                case CommitOptions.SealUpdatable:
                    BuildRootKeccakOrRlp();
                    _store.Seal();
                    break;
                case CommitOptions.ForceFlush:
                    BuildRootKeccakOrRlp();
                    _store.Seal();
                    _parent._lastFlushTo = _db.NextId - 1;
                    _db.FlushFrom(_lastFlushTo);
                    break;
                default:
                    throw new ArgumentOutOfRangeException(nameof(options), options, null);
            }
        }

        private void BuildRootKeccakOrRlp()
        {
            _parent._type = CalculateKeccakOrRlp(_db, _root, _parent._keccakOrRlp);
        }
    }

    internal interface IStore
    {
        public long TryUpdateOrAdd(long current, in Span<byte> written);

        ReadOnlySpan<byte> Read(long id);

        long Write(ReadOnlySpan<byte> payload);
    }

    class Store : IStore
    {
        private const int MaxCachedLength = 768;

        private readonly IDb _db;
        private long _updateFrom;
        private readonly long[] _slots;

        public Store(IDb db)
        {
            _db = db;
            _slots = new long[MaxCachedLength];
        }

        public void EnsureUpdatable()
        {
            if (_updateFrom == long.MaxValue)
            {
                _updateFrom = Math.Min(_db.NextId, _updateFrom);
            }
        }

        public void Seal()
        {
            _updateFrom = long.MaxValue;
            Array.Clear(_slots);
        }

        public long TryUpdateOrAdd(long current, in Span<byte> written)
        {
            Branch.AssertMemoryDb(written);
            
            var currentNode = _db.Read(current);

            if (current >= _updateFrom)
            {
                if (written.TryCopyTo(currentNode))
                {
                    return current;
                }
            }

            var length = currentNode.Length;

            // the current was not sufficient, cache it for future
            if (Id.Size <= length && length < MaxCachedLength)
            {
                // create a chain, write previous in node, put the current there
                BinaryPrimitives.WriteInt64LittleEndian(currentNode, _slots[length]);
                _slots[length] = current;
            }
            else
            {
                // not cacheable, free it
                _db.Free(current);
            }

            // current is no longer used, try to get a node from cache
            var writtenLength = written.Length;
            ref var slot = ref _slots[writtenLength];

            var nextId = _db.NextId;

            while (slot != Null)
            {
                var currentSlot = slot;
                var reusable = _db.Read(currentSlot);
                var next = BinaryPrimitives.ReadInt64LittleEndian(reusable);
                slot = next;

                // Only reuse slot if in the same file. Reusing a slot from an old file may result in a random access.
                if (Id.IsSameFile(nextId, currentSlot))
                {
                    written.CopyTo(reusable);
                    return currentSlot;
                }
            }

            // all caching failed, just write
            return _db.Write(written);
        }

        public ReadOnlySpan<byte> Read(long id) => _db.Read(id);

        public long Write(ReadOnlySpan<byte> payload)
        {
            Branch.AssertMemoryDb(payload);
            return _db.Write(payload);
        }
    }
}