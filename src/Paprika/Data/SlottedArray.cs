using System.Buffers;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Paprika.Crypto;
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
    public const int MinSize = AllocationGranularity * 3;

    private const int AllocationGranularity = 8;

    private readonly ref Header _header;
    private readonly Span<byte> _data;
    private readonly Span<Slot> _slots;
    private readonly Span<byte> _raw;

    public SlottedArray(Span<byte> buffer)
    {
        _raw = buffer;
        _header = ref Unsafe.As<byte, Header>(ref _raw[0]);
        _data = buffer.Slice(Header.Size);
        _slots = MemoryMarshal.Cast<byte, Slot>(_data);
    }

    public bool TrySet(in ReadOnlySpan<byte> key, ReadOnlySpan<byte> data, ushort? keyHash = default)
    {
        var hash = keyHash ?? GetHash(key);

        if (TryGetImpl(key, hash, out var existingData, out var index))
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
        var total = GetTotalSpaceRequired(key, data);

        if (_header.Taken + total + Slot.Size > _data.Length)
        {
            if (_header.Deleted == 0)
            {
                // nothing to reclaim
                return false;
            }

            // there are some deleted entries, run defragmentation of the buffer and try again
            Deframent();

            // re-evaluate again
            if (_header.Taken + total + Slot.Size > _data.Length)
            {
                // not enough memory
                return false;
            }
        }

        var at = _header.Low;
        ref var slot = ref _slots[at / Slot.Size];

        // write slot
        slot.Hash = hash;
        slot.ItemAddress = (ushort)(_data.Length - _header.High - total);
        slot.IsDeleted = false;

        // write item: length_key, key, data
        var dest = _data.Slice(slot.ItemAddress, total);

        int offset;

        if (key.Length <= Slot.MaxSlotLengthKey)
        {
            slot.KeyLength = key.Length;
            offset = 0;
        }
        else
        {
            slot.KeyLength = Slot.KeyLongerMarker;
            dest[0] = (byte)key.Length;
            offset = 1;
        }

        key.CopyTo(dest.Slice(offset));
        data.CopyTo(dest.Slice(offset + key.Length));

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
        /// <summary>The map being enumerated.</summary>
        private readonly SlottedArray _map;

        /// <summary>The next index to yield.</summary>
        private int _index;

        private readonly byte[] _bytes;
        private Item _current;

        internal Enumerator(SlottedArray map)
        {
            _map = map;
            _index = -1;
            _bytes = ArrayPool<byte>.Shared.Rent(128);
        }

        /// <summary>Advances the enumerator to the next element of the span.</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool MoveNext()
        {
            int index = _index + 1;
            var to = _map.Count;

            ref var slot = ref _map._slots[index];

            while (index < to && slot.IsDeleted) // filter out deleted
            {
                // move by 1
                index += 1;
                slot = ref Unsafe.Add(ref slot, 1);
            }

            if (index < to)
            {
                _index = index;
                _current = Build();
                return true;
            }

            return false;
        }

        public Item Current => _current;

        private Item Build()
        {
            ref var slot = ref _map._slots[_index];
            var span = _map.GetSlotPayload(ref slot);

            var shift = slot.KeyLength == Slot.KeyLongerMarker ? KeyLengthLength : 0;
            var keyLength = slot.KeyLength == Slot.KeyLongerMarker ? span[0] : slot.KeyLength;

            var key = span.Slice(shift, keyLength);
            var data = span.Slice(shift + keyLength);

            return new Item(key, data, _index);
        }

        public void Dispose()
        {
            if (_bytes != null)
                ArrayPool<byte>.Shared.Return(_bytes);
        }

        public readonly ref struct Item
        {
            public int Index { get; }
            public ReadOnlySpan<byte> Key { get; }
            public ReadOnlySpan<byte> RawData { get; }

            public Item(ReadOnlySpan<byte> key, ReadOnlySpan<byte> rawData, int index)
            {
                Index = index;
                Key = key;
                RawData = rawData;
            }
        }

        // a shortcut to not allocate, just copy the enumerator
        public Enumerator GetEnumerator() => this;
    }

    /// <summary>
    /// Tries to move as many items as possible from this map to the destination map.
    /// </summary>
    /// <remarks>
    /// Returns how many items were moved.
    /// </remarks>
    public int MoveTo(in SlottedArray destination)
    {
        var count = 0;

        foreach (var item in EnumerateAll())
        {
            // try copy all, even if one is not copyable the other might
            if (destination.TrySet(item.Key, item.RawData))
            {
                count++;
                Delete(item);
            }
        }

        return count;
    }

    public const int BucketCount = 16;

    /// <summary>
    /// Gets the aggregated count of entries per nibble.
    /// </summary>
    public void GatherCountStatistics(Span<ushort> buckets, NibbleSelector selector)
    {
        Debug.Assert(buckets.Length == BucketCount);

        var to = _header.Low / Slot.Size;
        for (var i = 0; i < to; i++)
        {
            ref var slot = ref _slots[i];

            // extract only not deleted and these which have at least one nibble
            if (slot.IsDeleted == false)
            {
                var payload = GetSlotPayload(ref slot);

                var shift = slot.KeyLength == Slot.KeyLongerMarker ? KeyLengthLength : 0;
                var keyLength = slot.KeyLength == Slot.KeyLongerMarker ? payload[0] : slot.KeyLength;

                var first = selector(payload.Slice(shift, keyLength));
                if (first < BucketCount)
                {
                    buckets[first] += 1;
                }
            }
        }
    }

    private const int KeyLengthLength = 1;

    private static int GetTotalSpaceRequired(in ReadOnlySpan<byte> key, ReadOnlySpan<byte> data)
    {
        return (key.Length <= Slot.MaxSlotLengthKey ? 0 : KeyLengthLength) +
               key.Length + data.Length;
    }

    /// <summary>
    /// Warning! This does not set any tombstone so the reader won't be informed about a delete,
    /// just will miss the value.
    /// </summary>
    public bool Delete(in ReadOnlySpan<byte> key)
    {
        if (TryGetImpl(key, GetHash(key), out _, out var index))
        {
            DeleteImpl(index);
            return true;
        }

        return false;
    }

    public void Delete(in Enumerator.Item item) => DeleteImpl(item.Index);

    private void DeleteImpl(int index)
    {
        // mark as deleted first
        _slots[index].IsDeleted = true;
        _header.Deleted++;

        // always try to compact after delete
        CollectTombstones();
    }

    private void Deframent()
    {
        // As data were fitting before, the will fit after so all the checks can be skipped
        var size = _raw.Length;
        var array = ArrayPool<byte>.Shared.Rent(size);
        var span = array.AsSpan(0, size);

        span.Clear();
        var copy = new SlottedArray(span);
        var count = _header.Low / Slot.Size;

        for (int i = 0; i < count; i++)
        {
            var copyFrom = _slots[i];
            if (copyFrom.IsDeleted == false)
            {
                var fromSpan = GetSlotPayload(ref _slots[i]);

                ref var copyTo = ref copy._slots[copy._header.Low / Slot.Size];

                // copy raw, no decoding
                var high = (ushort)(copy._data.Length - copy._header.High - fromSpan.Length);
                fromSpan.CopyTo(copy._data.Slice(high));

                copyTo.Hash = copyFrom.Hash;
                copyTo.ItemAddress = high;
                copyTo.KeyLength = copyFrom.KeyLength;

                copy._header.Low += Slot.Size;
                copy._header.High = (ushort)(copy._header.High + fromSpan.Length);
            }
        }

        // finalize by coping over to this
        span.CopyTo(_raw);

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

        while (index >= 0 && _slots[index].IsDeleted)
        {
            // undo writing low
            _header.Low -= Slot.Size;

            // undo writing high
            var slice = GetSlotPayload(ref _slots[index]);
            var total = slice.Length;
            _header.High = (ushort)(_header.High - total);

            // cleanup
            _slots[index] = default;
            _header.Deleted--;

            // move back by one to see if it's deleted as well
            index--;
        }
    }

    public bool TryGet(ReadOnlySpan<byte> key, out ReadOnlySpan<byte> data)
    {
        if (TryGetImpl(key, GetHash(key), out var span, out _))
        {
            data = span;
            return true;
        }

        data = default;
        return false;
    }

    [OptimizationOpportunity(OptimizationType.CPU,
        "key encoding is delayed but it might be called twice, here + TrySet")]
    private bool TryGetImpl(scoped in ReadOnlySpan<byte> key, ushort hash, out Span<byte> data, out int slotIndex)
    {
        var to = _header.Low / Slot.Size;

        // uses vectorized search, treating slots as a Span<ushort>
        // if the found index is odd -> found a slot to be queried

        const int notFound = -1;
        var span = MemoryMarshal.Cast<Slot, ushort>(_slots.Slice(0, to));

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

            if ((offset & Slot.PrefixUshortMask) == Slot.PrefixUshortMask)
            {
                var i = offset / 2;

                ref var slot = ref _slots[i];
                if (slot.IsDeleted == false)
                {
                    var actual = GetSlotPayload(ref slot);

                    // The StartsWith check assumes that all the keys have the same length.
                    var length = key.Length;
                    var shift = slot.KeyLength == Slot.KeyLongerMarker ? KeyLengthLength : 0;
                    var actualLength = slot.KeyLength == Slot.KeyLongerMarker ? actual[0] : slot.KeyLength;

                    if (actualLength == length)
                    {
                        if (actual.Slice(shift, length).SequenceEqual(key))
                        {
                            data = actual.Slice(shift + length);
                            slotIndex = i;
                            return true;
                        }
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
        var previousSlotAddress = Unsafe.IsAddressLessThan(ref _slots[0], ref slot)
            ? Unsafe.Add(ref slot, -1).ItemAddress
            : _data.Length;

        var length = previousSlotAddress - slot.ItemAddress;
        return _data.Slice(slot.ItemAddress, length);
    }

    [StructLayout(LayoutKind.Explicit, Size = Size)]
    private struct Slot
    {
        public const int Size = 4;

        // ItemAddress, requires 12 bits [0-11] to address whole page 
        private const ushort AddressMask = Page.PageSize - 1;

        /// <summary>
        /// The address of this item.
        /// </summary>
        public ushort ItemAddress
        {
            get => (ushort)(Raw & AddressMask);
            set => Raw = (ushort)((Raw & ~AddressMask) | value);
        }

        private const ushort DeletedMask = 0b0001_0000_0000_0000;

        /// <summary>
        /// The data type contained in this slot.
        /// </summary>
        public bool IsDeleted
        {
            get => (Raw & DeletedMask) == DeletedMask;
            set => Raw = (ushort)((Raw & ~DeletedMask) | (ushort)(value ? DeletedMask : 0));
        }

        private const ushort KeyLengthMask = 0b1110_0000_0000_0000;
        private const ushort KeyLengthShift = 13;
        public const int MaxSlotLengthKey = 6;
        public const int KeyLongerMarker = 7;

        public int KeyLength
        {
            get => (Raw & KeyLengthMask) >> KeyLengthShift;
            set => Raw = (ushort)((Raw & ~KeyLengthMask) | (value << KeyLengthShift));
        }

        [FieldOffset(0)] private ushort Raw;

        /// <summary>
        /// Used for vectorized search
        /// </summary>
        public const int PrefixUshortMask = 1;

        /// <summary>
        /// The memorized result of <see cref="GetHash"/> of this item.
        /// </summary>
        [FieldOffset(2)] public ushort Hash;

        public override string ToString()
        {
            return
                $"{nameof(Hash)}: {Hash}, {nameof(ItemAddress)}: {ItemAddress}";
        }
    }

    /// <summary>
    /// Builds the hash for the key. 
    /// </summary>
    /// <remarks>
    /// Highly optimized to eliminate bound checks and special cases
    /// </remarks>
    public static ushort GetHash(ReadOnlySpan<byte> key)
    {
        // First, special cases for short paths
        if (key.Length <= 2)
        {
            if (key.Length == 2)
            {
                return Unsafe.ReadUnaligned<ushort>(in key[0]);
            }

            return key.Length == 0 ? (ushort)0 : MemoryMarshal.GetReference(key);
        }

        // At least 3 bytes long, read start and end
        var a = Unsafe.ReadUnaligned<ushort>(in key[0]);
        var b = Unsafe.ReadUnaligned<ushort>(in key[^2]);

        // A really simple hash.
        return (ushort)(a ^ b);
    }

    public override string ToString() => $"{nameof(Count)}: {Count}, {nameof(CapacityLeft)}: {CapacityLeft}";

    public IEnumerable<string> MaterializeAllKeys()
    {
        var list = new List<string>();
        foreach (var item in EnumerateAll())
        {
            list.Add(item.Key.ToHexString(false));
        }

        list.Sort(StringComparer.InvariantCultureIgnoreCase);

        return list;
    }

    [StructLayout(LayoutKind.Explicit, Size = Size)]
    private struct Header
    {
        public const int Size = 8;

        /// <summary>
        /// Represents the distance from the start.
        /// </summary>
        [FieldOffset(0)] public ushort Low;

        /// <summary>
        /// Represents the distance from the end.
        /// </summary>
        [FieldOffset(2)] public ushort High;

        /// <summary>
        /// A rough estimates of gaps.
        /// </summary>
        [FieldOffset(4)] public ushort Deleted;

        public ushort Taken => (ushort)(Low + High);
    }
}