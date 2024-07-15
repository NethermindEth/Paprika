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
public readonly ref struct UShortSlottedArray
{
    private readonly ref Header _header;
    private readonly Span<byte> _data;
    private readonly Span<byte> _raw;

    public UShortSlottedArray(Span<byte> buffer)
    {
        _raw = buffer;
        _header = ref Unsafe.As<byte, Header>(ref _raw[0]);
        _data = buffer.Slice(Header.Size);
    }

    public bool TrySet(ushort key, ReadOnlySpan<byte> data)
    {
        if (TryGetImpl(key, out var existingData, out var index))
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
        var total = GetTotalSpaceRequired(data);

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
        ref var slot = ref this[at / Slot.Size];

        // write slot
        slot.Key = key;
        slot.ItemAddress = (ushort)(_data.Length - _header.High - total);
        slot.IsDeleted = false;

        // write item: length_key, key, data
        var dest = _data.Slice(slot.ItemAddress, total);

        data.CopyTo(dest);

        // commit low and high
        _header.Low += Slot.Size;
        _header.High += (ushort)total;

        return true;
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

    /// <summary>
    /// Gets how many slots are used in the map.
    /// </summary>
    public int Count => _header.Low / Slot.Size;

    /// <summary>
    /// Returns the capacity of the map.
    /// It includes slots that were deleted and that can be reclaimed when a defragmentation happens.
    /// </summary>
    public int CapacityLeft => _data.Length - _header.Taken + _header.Deleted;

    public bool CanAdd(in ReadOnlySpan<byte> data) => CapacityLeft >= Slot.Size + data.Length;

    private static int GetTotalSpaceRequired(ReadOnlySpan<byte> data) => data.Length;

    /// <summary>
    /// Warning! This does not set any tombstone so the reader won't be informed about a delete,
    /// just will miss the value.
    /// </summary>
    public bool Delete(ushort key)
    {
        if (TryGetImpl(key, out _, out var index))
        {
            DeleteImpl(index);
            return true;
        }

        return false;
    }

    private void DeleteImpl(int index)
    {
        // mark as deleted first
        ref var slot = ref this[index];
        slot.IsDeleted = true;

        var size = (ushort)(GetSlotLength(ref slot) + Slot.Size);

        Debug.Assert(_header.Deleted + size <= _data.Length, "Deleted marker breached size");

        _header.Deleted += size;

        // always try to compact after delete
        CollectTombstones();
    }

    private void Deframent()
    {
        // As data were fitting before, the will fit after so all the checks can be skipped
        var count = _header.Low / Slot.Size;

        // The pointer where the writing in the array ended, move it up when written.
        var writeAt = 0;
        var writtenTo = (ushort)_data.Length;
        var readTo = writtenTo;
        var newCount = (ushort)0;

        for (int i = 0; i < count; i++)
        {
            var slot = this[i];
            var addr = slot.ItemAddress;

            if (!slot.IsDeleted)
            {
                newCount++;

                if (writtenTo == readTo)
                {
                    // This is a case where nothing required copying so far, just move on by advancing it all.
                    writeAt++;
                    writtenTo = addr;
                }
                else
                {
                    // Something has been previously deleted, needs to be copied carefully
                    var source = _data.Slice(addr, readTo - addr);
                    writtenTo = (ushort)(writtenTo - source.Length);
                    var destination = _data.Slice(writtenTo, source.Length);
                    source.CopyTo(destination);
                    ref var destinationSlot = ref this[writeAt];

                    // Copy everything, just overwrite the address
                    destinationSlot.Key = slot.Key;
                    destinationSlot.ItemAddress = writtenTo;
                    destinationSlot.IsDeleted = false;

                    writeAt++;
                }
            }

            // Memoize to what is read to
            readTo = addr;
        }

        // Finalize by setting the header
        _header.Low = (ushort)(newCount * Slot.Size);
        _header.High = (ushort)(_data.Length - writtenTo);
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

            ref var slot = ref this[index];

            // undo writing high
            var slice = GetSlotPayload(ref slot);
            var total = slice.Length;
            _header.High = (ushort)(_header.High - total);

            // cleanup
            Debug.Assert(_header.Deleted >= total + Slot.Size, "Deleted marker breached size");

            _header.Deleted -= (ushort)(total + Slot.Size);

            slot = default;

            // move back by one to see if it's deleted as well
            index--;
        }
    }

    public bool TryGet(ushort key, out ReadOnlySpan<byte> data)
    {
        if (TryGetImpl(key, out var span, out _))
        {
            data = span;
            return true;
        }

        data = default;
        return false;
    }

    [OptimizationOpportunity(OptimizationType.CPU,
        "key encoding is delayed but it might be called twice, here + TrySet")]
    private bool TryGetImpl(ushort key, out Span<byte> data, out int slotIndex)
    {
        var to = _header.Low;

        // uses vectorized search, treating slots as a Span<ushort>
        // if the found index is odd -> found a slot to be queried
        const int notFound = -1;
        var span = MemoryMarshal.Cast<byte, ushort>(_data.Slice(0, to));

        var offset = 0;
        int index = span.IndexOf(key);

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

                ref var slot = ref this[i];
                if (slot.IsDeleted == false)
                {
                    data = GetSlotPayload(ref slot);
                    slotIndex = i;
                    return true;
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
            index = span.IndexOf(key);
        }

        data = default;
        slotIndex = default;
        return false;
    }

    /// <summary>
    /// Gets the payload pointed to by the given slot without the length prefix.
    /// </summary>
    private Span<byte> GetSlotPayload(ref Slot slot) => _data.Slice(slot.ItemAddress, GetSlotLength(ref slot));

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private ushort GetSlotLength(ref Slot slot)
    {
        // assert whether the slot has a previous, if not use data.length
        var previousSlotAddress = Unsafe.IsAddressLessThan(ref this[0], ref slot)
            ? Unsafe.Add(ref slot, -1).ItemAddress
            : _data.Length;

        return (ushort)(previousSlotAddress - slot.ItemAddress);
    }

    [StructLayout(LayoutKind.Sequential, Pack = sizeof(byte), Size = Size)]
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

        private ushort Raw;

        /// <summary>
        /// Used for vectorized search
        /// </summary>
        public const int PrefixUshortMask = 1;

        /// <summary>
        /// The key of the item.
        /// </summary>
        public ushort Key;

        public override string ToString() => $"{nameof(Key)}: {Key}, {nameof(ItemAddress)}: {ItemAddress}";
    }

    public override string ToString() => $"{nameof(Count)}: {Count}, {nameof(CapacityLeft)}: {CapacityLeft}";

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
