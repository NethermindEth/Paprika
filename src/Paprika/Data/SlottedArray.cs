using System.Buffers;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Paprika.Store;
using Paprika.Utils;

namespace Paprika.Data;

/// <summary>
/// Represents an in-page map, responsible for storing items and information related to them.
/// Allows for efficient nibble enumeration so that if a subset of items should be extracted, it's easy to do so.
/// </summary>
/// <remarks>
/// The map is fixed in since as it's page dependent, hence the name.
/// It is a modified version of a slot array, that does not externalize slot indexes.
///
/// It keeps an internal map, now implemented with a not-the-best loop over slots.
/// With the use of key prefix, it should be small enough and fast enough for now.
/// </remarks>
public readonly ref struct SlottedArray
{
    public const int Alignment = 8;
    public const int HeaderSize = Header.Size;

    private readonly ref Header _header;
    private readonly Span<byte> _data;

    public SlottedArray(Span<byte> buffer)
    {
        _header = ref Unsafe.As<byte, Header>(ref MemoryMarshal.GetReference(buffer));
        _data = buffer.Slice(Header.Size);
    }

    private readonly ref Slot this[int index]
    {
        get
        {
            var offset = index * Slot.Size;
            if (offset >= _data.Length - Slot.Size)
            {
                ThrowIndexOutOfRangeException();
            }

            return ref Unsafe.As<byte, Slot>(ref Unsafe.Add(ref MemoryMarshal.GetReference(_data), offset));

            [DoesNotReturn]
            [StackTraceHidden]
            static void ThrowIndexOutOfRangeException()
            {
                throw new IndexOutOfRangeException();
            }
        }
    }

    public bool TrySet(in NibblePath key, ReadOnlySpan<byte> data)
    {
        var hash = Slot.PrepareKey(key, out var preamble, out var trimmed);
        return TrySetImpl(hash, preamble, trimmed, data);
    }

    private bool TrySetImpl(ushort hash, byte preamble, in NibblePath trimmed, ReadOnlySpan<byte> data)
    {
        if (TryGetImpl(trimmed, hash, preamble, out var existingData, out var index))
        {
            // same size, copy in place
            if (data.Length == existingData.Length)
            {
                data.CopyTo(existingData);
                return true;
            }

            // cannot reuse, delete existing and add again
            DeleteImpl(index);
        }

        // does not exist yet, calculate total memory needed
        var total = GetTotalSpaceRequired(preamble, trimmed, data);

        if (_header.Taken + total + Slot.Size > _data.Length)
        {
            if (_header.Deleted == 0)
            {
                // nothing to reclaim
                return false;
            }

            // there are some deleted entries, run defragmentation of the buffer and try again
            Defragment();

            // re-evaluate again
            if (_header.Taken + total + Slot.Size > _data.Length)
            {
                // not enough memory
                return false;
            }
        }

        var at = _header.Low;
        ref var slot = ref this[at / Slot.Size];

        // write slot
        slot.Hash = hash;
        slot.KeyPreamble = preamble;
        slot.ItemAddress = (ushort)(_data.Length - _header.High - total);

        // write item: length_key, key, data
        var dest = _data.Slice(slot.ItemAddress, total);

        int len = preamble >> 1;
        bool includeLengthAndOddity = HasKeyBytes(preamble);

        if (len <= 4)
        {
            data.CopyTo(dest);
        }
        else
        {
            var dest2 = trimmed.WriteToWithLeftover(dest, includeLengthAndOddity);
            data.CopyTo(dest2);
        }

        // commit low and high
        _header.Low += Slot.Size;
        _header.High += (ushort)total;

        return true;
    }

    /// <summary>
    /// Gets how many slots are used in the map.
    /// </summary>
    public int Count => _header.Low / Slot.Size;

    public int CapacityLeft => _data.Length - _header.Taken;


    public Enumerator EnumerateAll() =>
        new(this);

    public ref struct Enumerator
    {
        [StructLayout(LayoutKind.Sequential, Pack = sizeof(byte), Size = Size)]
        private ref struct Chunk
        {
            private const int Size = 64;

            private byte _start;

            public Span<byte> Span => MemoryMarshal.CreateSpan(ref _start, Size);
        }

        /// <summary>The map being enumerated.</summary>
        private readonly SlottedArray _map;

        /// <summary>The next index to yield.</summary>
        private int _index;

        private Chunk _bytes;
        private Item _current;

        internal Enumerator(SlottedArray map)
        {
            _map = map;
            _index = -1;
        }

        /// <summary>Advances the enumerator to the next element of the span.</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool MoveNext()
        {
            int index = _index + 1;
            var to = _map.Count;

            ref var slot = ref _map[index];

            while (index < to && slot.IsDeleted) // filter out deleted
            {
                // move by 1
                index += 1;
                slot = ref Unsafe.Add(ref slot, 1);
            }

            if (index < to)
            {
                _index = index;
                Build(out _current);
                return true;
            }

            return false;
        }

        public readonly Item Current => _current;

        private void Build(out Item value)
        {
            ref var slot = ref _map[_index];
            var span = _map.GetSlotPayload(ref slot);
            var key = Slot.UnPrepareKey(slot.Hash, slot.KeyPreamble, span, _bytes.Span, out var data);

            value = new Item(key, data, _index);
        }

        public readonly void Dispose()
        {
        }

        public readonly ref struct Item(NibblePath key, ReadOnlySpan<byte> rawData, int index)
        {
            public int Index { get; } = index;
            public NibblePath Key { get; } = key;
            public ReadOnlySpan<byte> RawData { get; } = rawData;
        }

        // a shortcut to not allocate, just copy the enumerator
        public readonly Enumerator GetEnumerator() => this;
    }

    public void MoveNonEmptyKeysTo(in MapSource destination, bool treatEmptyAsTombstone = false)
    {
        var to = Count;
        var moved = 0;

        for (int i = 0; i < to; i++)
        {
            ref var slot = ref this[i];
            if (slot.IsDeleted)
                continue;

            if (slot.HasAtLeastOneNibble == false)
                continue;

            var nibble = Slot.GetFirstNibble(slot.Hash);
            ref readonly var map = ref MapSource.GetMap(destination, nibble);
            var payload = GetSlotPayload(ref slot);

            Span<byte> data;

            NibblePath trimmed;

            int len = slot.KeyPreamble >> 1;
            bool isOdd = (slot.KeyPreamble & 1) != 0;

            if (len is >= 5 and <= 6)
            {
                len -= 4;
                data = NibblePath.ReadFromWithLength(payload, len, isOdd, out trimmed);
            }
            else if (len >= 7)
            {
                data = NibblePath.ReadFrom(payload, out trimmed);
            }
            else
            {
                trimmed = default;
                data = payload;
            }

            if (data.IsEmpty && treatEmptyAsTombstone)
            {
                // special case for tombstones in overflows
                if (map.TryGetImpl(trimmed, slot.Hash, slot.KeyPreamble, out _, out var index))
                {
                    map.DeleteImpl(index);
                }
                slot.MarkAsDeleted();
            }
            else if (map.TrySetImpl(slot.Hash, slot.KeyPreamble, trimmed, data))
            {
                slot.MarkAsDeleted();
                moved++;
            }
        }

        if (moved > 0)
        {
            CollectTombstones();
            Defragment();
        }
    }

    public const int BucketCount = 16;

    /// <summary>
    /// Gets the aggregated count of entries per nibble.
    /// </summary>
    public void GatherCountStatistics(Span<ushort> buckets)
    {
        Debug.Assert(buckets.Length == BucketCount);

        var to = _header.Low / Slot.Size;
        for (var i = 0; i < to; i++)
        {
            ref var slot = ref this[i];

            // extract only not deleted and these which have at least one nibble
            // if (slot.IsDeleted == false && slot.HasAtLeastOneNibble)
            if (slot is { IsDeleted: false, HasAtLeastOneNibble: true }) // Just trying this out, unrelated change in syntax only, no relation to PR
            {
                buckets[slot.Nibble0Th] += 1;
            }
        }
    }

    private const int KeyLengthLength = 1;

    /// <summary>
    /// Determines the total space required for the key and data, taking into account specific length conditions.
    /// </summary>
    /// <param name="preamble">The preamble byte that contains key length and oddity information.</param>
    /// <param name="key">The NibblePath key.</param>
    /// <param name="data">The data associated with the key.</param>
    /// <returns>The total space required in bytes.</returns>
    private static int GetTotalSpaceRequired(byte preamble, in NibblePath key, ReadOnlySpan<byte> data)
    {
        return data.Length + (HasLengthFiveToSix(preamble) ? key.RawSpanLength : 0) +
               (HasKeyBytes(preamble) ? KeyLengthLength + key.RawSpanLength : 0);
    }

    /// <summary>
    /// Checks whether the preamble point that the key might have more data.
    /// </summary>
    private static bool HasKeyBytes(byte preamble) => preamble >= Slot.KeyPreambleWithBytes;

    /// <summary>
    /// Checks whether the preamble signifies that the length of the NibblePath key is 4, 5, or 6.
    /// </summary>
    private static bool HasLengthFiveToSix(byte preamble)
    {
        // Extract the length marker from the preamble
        byte lengthMarker = (byte)(preamble >> 1); // Remove the oddity bit

        // Check if the length marker matches 4, 5, or 6 (0b100, 0b101, 0b110)
        // return lengthMarker == 0b100 || lengthMarker == 0b101 || lengthMarker == 0b110;

        return lengthMarker is >= 5 and <= 6;
    }

    /// <summary>
    /// Warning! This does not set any tombstone so the reader won't be informed about a delete,
    /// just will miss the value.
    /// </summary>
    public bool Delete(in NibblePath key)
    {
        var hash = Slot.PrepareKey(key, out var preamble, out var trimmed);
        if (TryGetImpl(trimmed, hash, preamble, out _, out var index))
        {
            DeleteImpl(index);
            return true;
        }

        return false;
    }

    public void Delete(in Enumerator.Item item) => DeleteImpl(item.Index);

    private void DeleteImpl(int index, bool collectTombstones = true)
    {
        // mark as deleted first
        this[index].MarkAsDeleted();
        _header.Deleted++;

        if (collectTombstones)
        {
            CollectTombstones();
        }
    }

    private void Defragment()
    {
        // As data were fitting before, the will fit after so all the checks can be skipped
        var size = Header.Size + _data.Length;
        var array = ArrayPool<byte>.Shared.Rent(size);
        var span = array.AsSpan(0, size);

        // Create the slotted array over the dirty span and then clear it.
        // It's much cheaper than clearing the whole span itself.
        var copy = new SlottedArray(span);
        copy.Clear();

        var count = _header.Low / Slot.Size;

        for (int i = 0; i < count; i++)
        {
            var copyFrom = this[i];
            if (copyFrom.IsDeleted == false)
            {
                var fromSpan = GetSlotPayload(ref this[i]);

                ref var copyTo = ref copy[copy._header.Low / Slot.Size];

                // copy raw, no decoding
                var high = (ushort)(copy._data.Length - copy._header.High - fromSpan.Length);
                fromSpan.CopyTo(copy._data.Slice(high));

                copyTo.Hash = copyFrom.Hash;
                copyTo.ItemAddress = high;
                copyTo.KeyPreamble = copyFrom.KeyPreamble;

                copy._header.Low += Slot.Size;
                copy._header.High = (ushort)(copy._header.High + fromSpan.Length);
            }
        }

        // finalize by coping over to this
        var raw = MemoryMarshal.CreateSpan(ref Unsafe.As<Header, byte>(ref _header), Header.Size + _data.Length);
        span.CopyTo(raw);

        ArrayPool<byte>.Shared.Return(array);
        Debug.Assert(copy._header.Deleted == 0, "All deleted should be gone");
    }

    /// <summary>
    /// Collects tombstones of entities that used to be. 
    /// </summary>
    private void CollectTombstones()
    {
        // start with the last written and perform checks and cleanup till all the deleted are gone
        var index = Count - 1;

        while (index >= 0 && this[index].IsDeleted)
        {
            // undo writing low
            _header.Low -= Slot.Size;

            // undo writing high
            var slice = GetSlotPayload(ref this[index]);
            var total = slice.Length;
            _header.High = (ushort)(_header.High - total);

            // cleanup
            this[index] = default;
            _header.Deleted--;

            // move back by one to see if it's deleted as well
            index--;
        }
    }

    public bool TryGet(scoped in NibblePath key, out ReadOnlySpan<byte> data)
    {
        var hash = Slot.PrepareKey(key, out byte preamble, out var trimmed);
        if (TryGetImpl(trimmed, hash, preamble, out var span, out _))
        {
            data = span.IsEmpty ? ReadOnlySpan<byte>.Empty : MemoryMarshal.CreateReadOnlySpan(ref span[0], span.Length);
            return true;
        }

        data = default;
        return false;
    }

    /// <summary>
    /// Clears the map.
    /// </summary>
    private void Clear() // IDE suggestion, not related to PR
    {
        _header = default;
    }

    [OptimizationOpportunity(OptimizationType.CPU,
        "key encoding is delayed but it might be called twice, here + TrySet")]
    private bool TryGetImpl(in NibblePath key, ushort hash, byte preamble, out Span<byte> data, out int slotIndex)
    {
        var to = _header.Low;

        // uses vectorized search, treating slots as a Span<ushort>
        // if the found index is odd -> found a slot to be queried

        const int notFound = -1;
        var span = MemoryMarshal.Cast<byte, ushort>(_data.Slice(0, to));

        var offset = 0;
        int index = span.IndexOf(hash);

        if (index == notFound)
        {
            data = default;
            slotIndex = default;
            return false;
        }

        while (index != notFound)
        {
            // move offset to the given position
            offset += index;

            if ((offset & Slot.HashShiftForSearch) == Slot.HashShiftForSearch)
            {
                var i = offset / 2;

                ref var slot = ref this[i];
                if (slot.IsDeleted == false && slot.KeyPreamble == preamble)
                {
                    var actual = GetSlotPayload(ref slot);

                    if (slot.GetHasMidLength())
                    {
                        int len = (slot.KeyPreamble >> 1) - 4; // Length of trimmed path = length of key - length of hash
                        bool isOdd = (slot.KeyPreamble & 1) != 0;

                        if (NibblePath.TryReadFromWithLength(actual, key, len, isOdd, out var leftover))
                        {
                            data = leftover;
                            slotIndex = i;
                            return true;
                        }
                    }
                    if (slot.GetHasKeyBytes())
                    {
                        if (NibblePath.TryReadFrom(actual, key, out var leftover))
                        {
                            data = leftover;
                            slotIndex = i;
                            return true;
                        }
                    }
                    else
                    {
                        // The key is contained in the hash, all is equal and good to go!
                        data = actual;
                        slotIndex = i;
                        return true;
                    }
                }
            }

            if (index + 1 >= span.Length)
            {
                // the span is empty and there's not place to move forward
                break;
            }

            // move next: ushorts sliced to the next
            // offset moved by 1 to align
            span = span.Slice(index + 1);
            offset += 1;

            // move to next index
            index = span.IndexOf(hash);
        }

        data = default;
        slotIndex = default;
        return false;
    }

    /// <summary>
    /// Gets the payload pointed to by the given slot without the length prefix.
    /// </summary>
    private Span<byte> GetSlotPayload(ref Slot slot)
    {
        // assert whether the slot has a previous, if not use data.length
        var previousSlotAddress = Unsafe.IsAddressLessThan(ref this[0], ref slot)
            ? Unsafe.Add(ref slot, -1).ItemAddress
            : _data.Length;

        var length = previousSlotAddress - slot.ItemAddress;
        return _data.Slice(slot.ItemAddress, length);
    }

    /// <summary>
    /// Gets the payload pointed to by the given slot with the specified length.
    /// </summary>
    // private Span<byte> GetSlotPayloadWithLength(ref Slot slot, bool isOdd, int length)
    // {
    //     // Determine the actual length of the payload using the provided length
    //     var payloadLength = (length + (isOdd ? 1 : 0));
    //
    //     // Return the slice of _data representing the payload for the current slot
    //     return _data.Slice(slot.ItemAddress, payloadLength);
    // }

    /// <summary>
    /// Exposes <see cref="Slot.PrepareKey"/> for tests only.
    /// </summary>
    public static ushort HashForTests(in NibblePath key) => Slot.PrepareKey(key, out _, out _);

    /// <summary>
    /// The slot is a size of <see cref="Size"/> bytes.
    ///
    /// It consists of two ushort parts,
    /// 1. <see cref="Raw"/> and
    /// 2. <see cref="Hash"/>.
    ///
    /// <see cref="Hash"/> is a result of <see cref="PrepareKey"/> that returns the value to be memoized in a slot. It only 2 bytes so collision may occur.
    /// <see cref="Raw"/> encodes all the metadata related to the slot.
    /// </summary>
    [StructLayout(LayoutKind.Sequential, Pack = sizeof(byte), Size = Size)]
    private struct Slot
    {
        public const int Size = 4;

        /// <summary>
        /// The address currently requires 12 bits [0-11] to address whole page. 
        /// </summary>
        private const ushort AddressMask = Page.PageSize - 1; // 0x0FFF = 0b1111_1111_1111

        /// <summary>
        /// The address of this item.
        /// </summary>
        public ushort ItemAddress
        {
            readonly get => (ushort)(Raw & AddressMask);  // ItemAddress = 0b0000_0000_0000_xxxx & 0b0000_1111_1111_1111 = 0b0000_0000_0000_xxxx
            set => Raw = (ushort)((Raw & ~AddressMask) | value); // ItemAddress = 0b1111_1111_1111_xxxx
        }

        /// <summary>
        /// Whether the given entry is deleted or not
        /// </summary>
        public bool IsDeleted => KeyPreamble == KeyPreambleDelete;

        /// <summary>
        /// Marks the slot as deleted
        /// </summary>
        public void MarkAsDeleted() => KeyPreamble = KeyPreambleDelete;

        // Preamble uses all bits that AddressMask does not
        private const ushort KeyPreambleMask = unchecked((ushort)~AddressMask); // 0b1111_0000_0000_0000
        private const ushort KeyPreambleShift = 12;

        private const byte KeyPreambleLen4 = 0b0100; // 4 nibbles
        private const byte KeyPreambleLen5 = 0b101; // 5 nibbles
        private const byte KeyPreambleLen6 = 0b110; // 6 nibbles

        // The new (temporary KeyPreambleBeyond) is used for 7 and more nibbles, instead of 5 and more.
        // private const byte KeyPreambleLen7AndMore = 0b111; // 7 and more nibbles

        private const byte KeyPreambleBeyond = 0b111; // Some key nibbles are stored along data, this is the marker.
        private const byte KeyPreambleEmpty = 0b000; // Empty, no key's nibbles encoded.
        private const byte KeyPreambleOddBit = 0b001; // The bit used for odd-starting paths.
        private const byte KeyPreambleDelete = KeyPreambleOddBit; // Empty cannot be odd, odd is used as deleted marker.
        // 0b110, 0b111 are not used

        public const byte KeyPreambleWithBytes = KeyPreambleBeyond << KeyPreambleLengthShift;

        private const byte KeyPreambleLengthShift = 1;

        private const byte KeyPreambleMaxEncodedLength = KeyPreambleBeyond - 1;
        private const byte MaxLengthEncodedInHash = 4;
        private const byte KeySlice = 2;

        private const int HashByteShift = 8;

        public bool HasAtLeastOneNibble => KeyPreamble != KeyPreambleEmpty;

        public byte KeyPreamble
        {
            // KeyPreambleMask = 0b1111_0000_0000_0000
            readonly get => (byte)((Raw & KeyPreambleMask) >> KeyPreambleShift); // KeyPreamble = 0bxxxx_0000_0000_0000 >> 12
            set => Raw = (ushort)((Raw & ~KeyPreambleMask) | (value << KeyPreambleShift)); // KeyPreamble = 0bxxxx_0000_0000_0000 | (value << 12)
        }

        public readonly byte Nibble0Th => (byte)(Hash >> (HashByteShift + NibblePath.NibbleShift) & 0xF);

        public bool GetHasKeyBytes() => KeyPreamble >= KeyPreambleWithBytes;

        public bool GetHasMidLength() => (KeyPreamble >> 1) is >= KeyPreambleLen5 and <= KeyPreambleLen6;

        private ushort Raw;

        /// <summary>
        /// Used for vectorized search
        /// </summary>
        public const int HashShiftForSearch = 1;

        /// <summary>
        /// The memorized result of <see cref="PrepareKey"/> of this item.
        /// </summary>
        public ushort Hash;

        public override readonly string ToString()
        {
            return
                $"{nameof(Hash)}: {Hash}, {nameof(ItemAddress)}: {ItemAddress}";
        }

        /// <summary>
        /// Prepares the key for the search. 
        /// </summary>
        public static ushort PrepareKey(in NibblePath key, out byte preamble, out NibblePath trimmed)
        {
            const int shift = NibblePath.NibbleShift;

            var length = key.Length;
            var oddBit = key.IsOdd ? 1 : 0;

            if (length <= MaxLengthEncodedInHash) // If len <= 4, we can fit the entire key in the hash, no trimmed
            {
                preamble = (byte)((length << KeyPreambleLengthShift) | oddBit);
                trimmed = NibblePath.Empty;

                switch (length)
                {
                    // produce hashes aligned with NibblePath ordering
                    case 0:
                        preamble = 0; // no oddity for empty
                        return 0;
                    case 1:
                        return (ushort)(key.GetAt(0) << (shift + HashByteShift));
                    case 2:
                        return (ushort)(((key.GetAt(0) << shift) | key.GetAt(1)) << HashByteShift);
                    case 3:
                        return (ushort)((((key.GetAt(0) << shift) | key.GetAt(1)) << HashByteShift) |
                                        (key.GetAt(2) << shift));
                    case 4:
                        return (ushort)((((key.GetAt(0) << shift) | key.GetAt(1)) << HashByteShift) |
                                        (key.GetAt(2) << shift) | key.GetAt(3));
                }
            }

            // If len <= 6, we can fit the actual length in the preamble
            preamble = (length <= KeyPreambleMaxEncodedLength)
                ? (byte)((length << KeyPreambleLengthShift) | oddBit) // 0bxxx0 | oddBit
                : (byte)(KeyPreambleWithBytes | oddBit); // 0b111 | oddBit

            trimmed = key.SliceFrom(KeySlice).SliceTo(length - MaxLengthEncodedInHash); // 0xABCDEFGH => Hash = 0xABGH , trimmed = 0xCDEF

            // Extract first 2 nibbles and last 2 nibbles as the hash to avoid collisions due to common prefixes
            return (ushort)((((key.GetAt(0) << shift) | key.GetAt(1)) << HashByteShift) |
                            (key.GetAt(length - 2) << shift) | key.GetAt(length - 1));
        }

        [SkipLocalsInit]
        public static NibblePath UnPrepareKey(ushort hash, byte preamble, ReadOnlySpan<byte> input,
            Span<byte> workingSet, out ReadOnlySpan<byte> data)
        {
            var count = preamble >> KeyPreambleLengthShift;
            if (count == 0)
            {
                data = input;
                return default;
            }
            if (count is > 2 and <= 4)
            {
                Unsafe.As<byte, ushort>(ref MemoryMarshal.GetReference(workingSet))
                    = (ushort)((hash >> HashByteShift) | (hash << HashByteShift));
            }
            else // count <= 2 || count > 4
            {
                workingSet[0] = (byte)(hash >> HashByteShift);
                if (count <= 6)
                {
                    workingSet[1] = (byte)(hash & 0xFF);
                }
            }

            // if (count <= 2 || count > 6)
            // {
            //     workingSet[0] = (byte)(hash >> HashByteShift);
            // }
            // else  if (count <= 4) // For length 3,4 logic remains same
            // {
            //     Unsafe.As<byte, ushort>(ref MemoryMarshal.GetReference(workingSet))
            //         = (ushort)((hash >> HashByteShift) | (hash << HashByteShift));
            // }
            // else // For length 5, 6, suppose hash = 0xABCD (2 bytes)
            // {
            //     workingSet[0] = (byte)(hash >> HashByteShift); // 0x00AB = 0xAB
            //     workingSet[1] = (byte)(hash & 0xFF); // 0x00CD = 0xCD
            // }

            NibblePath prefix = NibblePath.FromKey(workingSet, 0, count > 4 ? KeySlice : count);
            if ((preamble & KeyPreambleOddBit) != 0)
            {
                prefix.UnsafeMakeOdd(); // moving odd can make move beyond 0th
            }

            if (count <= 4)
            {
                data = input;
                return prefix;
            }

            const int limit = 3;
            bool oddFlag = (preamble & KeyPreambleOddBit) != 0;

            data = count <= KeyPreambleMaxEncodedLength
                ? NibblePath.ReadFromWithLength(input, count - 4, oddFlag, out var trimmed)
                : NibblePath.ReadFrom(input, out trimmed);

            return prefix.Append(trimmed, hash, workingSet[limit..]);
        }

        public static byte GetFirstNibble(ushort hash)
        {
            const int shift = NibblePath.NibbleShift;
            return (byte)(hash >> (shift + HashByteShift));
        }
    }


    public override string ToString() => $"{nameof(Count)}: {Count}, {nameof(CapacityLeft)}: {CapacityLeft}";

    [StructLayout(LayoutKind.Sequential, Pack = sizeof(byte), Size = Size)]
    private struct Header
    {
        public const int Size = 8;

        /// <summary>
        /// Represents the distance from the start.
        /// </summary>
        public ushort Low;

        /// <summary>
        /// Represents the distance from the end.
        /// </summary>
        public ushort High;

        /// <summary>
        /// A rough estimates of gaps.
        /// </summary>
        public ushort Deleted;

        public readonly ushort Taken => (ushort)(Low + High);
    }
}

public readonly ref struct MapSource
{
    private readonly SlottedArray _map0;
    private readonly SlottedArray _map1;
    private readonly SlottedArray _map2;
    private readonly SlottedArray _map3;
    private readonly SlottedArray _map4;
    private readonly SlottedArray _map5;
    private readonly SlottedArray _map6;
    private readonly SlottedArray _map7;
    private readonly int _count;

    public MapSource(SlottedArray map)
    {
        _map0 = map;
        _count = 1;
    }

    public MapSource(SlottedArray map0, SlottedArray map1)
    {
        _map0 = map0;
        _map1 = map1;
        _count = 2;
    }

    public MapSource(SlottedArray map0, SlottedArray map1, SlottedArray map2, SlottedArray map3)
    {
        _map0 = map0;
        _map1 = map1;
        _map2 = map2;
        _map3 = map3;
        _count = 4;
    }

    public MapSource(SlottedArray map0, SlottedArray map1, SlottedArray map2, SlottedArray map3, SlottedArray map4,
        SlottedArray map5, SlottedArray map6, SlottedArray map7)
    {
        _map0 = map0;
        _map1 = map1;
        _map2 = map2;
        _map3 = map3;
        _map4 = map4;
        _map5 = map5;
        _map6 = map6;
        _map7 = map7;
        _count = 8;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static ref readonly SlottedArray GetMap(in MapSource source, int nibble)
    {
        switch (nibble % source._count)
        {
            case 0:
                return ref source._map0;
            case 1:
                return ref source._map1;
            case 2:
                return ref source._map2;
            case 3:
                return ref source._map3;
            case 4:
                return ref source._map4;
            case 5:
                return ref source._map5;
            case 6:
                return ref source._map6;
            default:
                return ref source._map7;
        }
    }
}
