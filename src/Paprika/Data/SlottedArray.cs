using System.Buffers.Binary;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Numerics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Runtime.Intrinsics;
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
/// It keeps an internal map, that is aligned with the local hardware vector size, so that even vectors (0th, 2nd, 4th...)
/// are used for hashes, while odd (1st, 3rd, 5th...) are used to store slots.
/// </remarks>
public readonly ref struct SlottedArray
{
    public const int Alignment = 8;
    public const int HeaderSize = Header.Size;

    private readonly ref Header _header;
    private readonly Span<byte> _data;

    private static readonly int VectorSize =
        Vector256.IsHardwareAccelerated ? Vector256<byte>.Count : Vector128<byte>.Count;

    private const int VectorsByBatch = 2;
    private static readonly int DoubleVectorSize = VectorSize * VectorsByBatch;

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static int AlignToDoubleVectorSize(int count) => (count + (DoubleVectorSize - 1)) & -DoubleVectorSize;

    public SlottedArray(Span<byte> buffer)
    {
        Debug.Assert(buffer.Length > MinimalSizeWithNoData,
            $"The buffer should be reasonably big, more than {MinimalSizeWithNoData}");

        _header = ref Unsafe.As<byte, Header>(ref MemoryMarshal.GetReference(buffer));
        _data = buffer.Slice(Header.Size);
    }

    public static int MinimalSizeWithNoData => DoubleVectorSize + Header.Size;

    private ref ushort GetHashRef(int index)
    {
        // Hashes are at [0, VectorSize), then [VectorSize*2, VectorSize*3), then [VectorSize*4, VectorSize*5)
        // To extract them extract the higher part and multiply by two, then add the lower part.

        var uShortsPerVector = VectorSize / 2;
        var mask = uShortsPerVector - 1;
        var offset = (index & ~mask) * 2 + (index & mask);

        return ref Unsafe.Add(ref Unsafe.As<byte, ushort>(ref MemoryMarshal.GetReference(_data)), offset);
    }

    private ref Slot GetSlotRef(int index)
    {
        // Slots are at [VectorSize, VectorSize*2), then [VectorSize*3, VectorSize*4), then [VectorSize*5, VectorSize*6) 
        // To extract them extract the higher part and multiply by two, then add the lower part.
        // Additionally, add one ushorts per vector
        var uShortsPerVector = VectorSize / 2;

        var mask = uShortsPerVector - 1;
        var offset = (index & ~mask) * 2 + (index & mask) + uShortsPerVector;

        return ref Unsafe.Add(ref Unsafe.As<byte, Slot>(ref MemoryMarshal.GetReference(_data)), offset);
    }

    public bool TrySet(in NibblePath key, ReadOnlySpan<byte> data)
    {
        var hash = Slot.PrepareKey(key, out var preamble, out var trimmed);
        return TrySetImpl(hash, preamble, trimmed, data);
    }

    private bool TrySetImpl(ushort hash, byte preamble, in NibblePath trimmed, ReadOnlySpan<byte> data)
    {
        var index = TryGetImpl(trimmed, hash, preamble, out var existingData);
        if (index != NotFound)
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

        if (_header.TakenAfterOneMoreSlot + total > _data.Length)
        {
            if (_header.Deleted == 0)
            {
                // nothing to reclaim
                return false;
            }

            // there are some deleted entries, run defragmentation of the buffer and try again
            Defragment();

            // re-evaluate again
            if (_header.TakenAfterOneMoreSlot + total > _data.Length)
            {
                // not enough memory
                return false;
            }
        }

        var at = _header.Low / Slot.TotalSize;

        // Write hash at its place
        GetHashRef(at) = hash;

        // Writing slot at its place
        ref var slot = ref GetSlotRef(at);
        slot.KeyPreamble = preamble;
        slot.ItemAddress = (ushort)(_data.Length - _header.High - total);

        // write item: length_key, key, data
        var dest = _data.Slice(slot.ItemAddress, total);

        if (HasKeyBytes(preamble))
        {
            var dest2 = trimmed.WriteToWithLeftover(dest);
            data.CopyTo(dest2);
        }
        else
        {
            data.CopyTo(dest);
        }

        // commit low and high
        _header.Low += Slot.TotalSize;
        _header.High += (ushort)total;

        return true;
    }

    /// <summary>
    /// Gets how many slots are used in the map.
    /// </summary>
    public int Count => _header.Low / Slot.TotalSize;

    public int CapacityLeft => _data.Length - _header.Taken;


    public Enumerator EnumerateAll() =>
        new(this);

    public ref struct Enumerator
    {
        [StructLayout(LayoutKind.Sequential, Pack = sizeof(byte), Size = Size)]
        private ref struct Chunk
        {
            public const int Size = 64;

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

            ref var slot = ref _map.GetSlotRef(index);

            while (index < to && slot.IsDeleted) // filter out deleted
            {
                // move by 1
                index += 1;
                slot = ref _map.GetSlotRef(index);
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
            ref var slot = ref _map.GetSlotRef(_index);
            var hash = _map.GetHashRef(_index);

            var span = _map.GetSlotPayload(_index);
            var key = Slot.UnPrepareKey(hash, slot.KeyPreamble, span, _bytes.Span, out var data);

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
            ref var slot = ref GetSlotRef(i);
            if (slot.IsDeleted)
                continue;

            if (slot.HasAtLeastOneNibble == false)
                continue;

            var nibble = slot.GetNibble0(GetHashRef(i));
            ref readonly var map = ref MapSource.GetMap(destination, nibble);
            var payload = GetSlotPayload(i);

            Span<byte> data;

            NibblePath trimmed;
            if (slot.HasKeyBytes)
            {
                data = NibblePath.ReadFrom(payload, out trimmed);
            }
            else
            {
                trimmed = default;
                data = payload;
            }

            var hash = GetHashRef(i);
            if (data.IsEmpty && treatEmptyAsTombstone)
            {
                // special case for tombstones in overflows
                var index = map.TryGetImpl(trimmed, hash, slot.KeyPreamble, out _);
                if (index != NotFound)
                {
                    map.DeleteImpl(index);
                }

                slot.MarkAsDeleted();
            }
            else if (map.TrySetImpl(hash, slot.KeyPreamble, trimmed, data))
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

        var to = _header.Low / Slot.TotalSize;
        for (var i = 0; i < to; i++)
        {
            ref var slot = ref GetSlotRef(i);

            // extract only not deleted and these which have at least one nibble
            if (slot.IsDeleted == false && slot.HasAtLeastOneNibble)
            {
                buckets[slot.GetNibble0(GetHashRef(i))] += 1;
            }
        }
    }

    private const int KeyLengthLength = 1;

    private static int GetTotalSpaceRequired(byte preamble, in NibblePath key, ReadOnlySpan<byte> data)
    {
        return (HasKeyBytes(preamble) ? KeyLengthLength + key.RawSpanLength : 0) + data.Length;
    }

    /// <summary>
    /// Checks whether the preamble point that the key might have more data.
    /// </summary>
    private static bool HasKeyBytes(byte preamble) => preamble >= Slot.KeyPreambleWithBytes;

    /// <summary>
    /// Warning! This does not set any tombstone so the reader won't be informed about a delete,
    /// just will miss the value.
    /// </summary>
    public bool Delete(in NibblePath key)
    {
        var hash = Slot.PrepareKey(key, out var preamble, out var trimmed);
        var index = TryGetImpl(trimmed, hash, preamble, out _);
        if (index != NotFound)
        {
            DeleteImpl(index);
            return true;
        }

        return false;
    }

    public void Delete(in Enumerator.Item item) => DeleteImpl(item.Index);

    private void DeleteImpl(int index, bool collectTombstones = true)
    {
        // Mark as deleted first
        MarkAsDeleted(index);
        _header.Deleted++;

        if (collectTombstones)
        {
            CollectTombstones();
        }
    }

    private void MarkAsDeleted(int index)
    {
        GetSlotRef(index).MarkAsDeleted();

        // Provide a different hash so that further searches with TryGet won't be hitting this slot.
        //
        // We could use a constant value, but then on a collision with an actual value the tail
        // performance would be terrible.
        //
        // The easiest way is to negate the hash that makes it not equal and yet is not a single value.
        ref var hash = ref GetHashRef(index);
        hash = (ushort)~hash;
    }

    private void Defragment()
    {
        // As data were fitting before, the will fit after so all the checks can be skipped
        var count = _header.Low / Slot.TotalSize;

        // The pointer where the writing in the array ended, move it up when written.
        var writeAt = 0;
        var writtenTo = (ushort)_data.Length;
        var readTo = writtenTo;
        var newCount = (ushort)0;

        for (int i = 0; i < count; i++)
        {
            ref var slot = ref GetSlotRef(i);
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
                    ref var destinationSlot = ref GetSlotRef(writeAt);

                    // Copy hash
                    GetHashRef(writeAt) = GetHashRef(i);

                    // Copy everything, just overwrite the address
                    destinationSlot.KeyPreamble = slot.KeyPreamble;
                    destinationSlot.ItemAddress = writtenTo;

                    writeAt++;
                }
            }

            // Memoize to what is read to
            readTo = addr;
        }

        // Finalize by setting the header
        _header.Low = (ushort)(newCount * Slot.TotalSize);
        _header.High = (ushort)(_data.Length - writtenTo);
        _header.Deleted = 0;
    }

    /// <summary>
    /// Collects tombstones of entities that used to be. 
    /// </summary>
    private void CollectTombstones()
    {
        // start with the last written and perform checks and cleanup till all the deleted are gone
        var index = Count - 1;

        while (index >= 0 && GetSlotRef(index).IsDeleted)
        {
            // undo writing low
            _header.Low -= Slot.TotalSize;

            // undo writing high
            var slice = GetSlotPayload(index);
            var total = slice.Length;
            _header.High = (ushort)(_header.High - total);

            // cleanup
            // Hash is already replaced with its delete. Clean the slot
            GetSlotRef(index) = default;
            _header.Deleted--;

            // move back by one to see if it's deleted as well
            index--;
        }
    }

    public bool TryGet(scoped in NibblePath key, out ReadOnlySpan<byte> data)
    {
        var hash = Slot.PrepareKey(key, out byte preamble, out var trimmed);
        if (TryGetImpl(trimmed, hash, preamble, out var span) != NotFound)
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
    public void Clear()
    {
        _header = default;
    }

    private const int NotFound = -1;

    [OptimizationOpportunity(OptimizationType.CPU,
        "key encoding is delayed but it might be called twice, here + TrySet")]
    private int TryGetImpl(in NibblePath key, ushort hash, byte preamble, out Span<byte> data)
    {
        var count = _header.Low / Slot.TotalSize;
        var jump = DoubleVectorSize / sizeof(ushort);
        var aligned = AlignToDoubleVectorSize(_header.Low) / sizeof(ushort);

        ref var d = ref Unsafe.As<byte, ushort>(ref MemoryMarshal.GetReference(_data));

        if (Vector256.IsHardwareAccelerated)
        {
            var search = Vector256.Create(hash);

            for (var i = 0; i < aligned; i += jump)
            {
                var value = Vector256.LoadUnsafe(ref d, (UIntPtr)i);
                if (Vector256.EqualsAny(value, search))
                {
                    var matches = Vector256.Equals(value, search).ExtractMostSignificantBits();

                    if (i + jump >= aligned)
                    {
                        // Undoing the multiplication done above to calculate aligned, to get the number of items.
                        var alignedCount = aligned / VectorsByBatch;
                        var toClear = alignedCount - count;

                        // This is the last in batch, masking is required to remove potential hits that are false positive
                        var hashesPerVector = VectorSize / sizeof(ushort);
                        var mask = (1U << hashesPerVector - toClear) - 1;
                        matches &= mask;
                    }

                    if (matches > 0)
                    {
                        var found = TryFind(i / VectorsByBatch, matches, key, preamble, out data);
                        if (found != NotFound)
                        {
                            return found;
                        }
                    }
                }
            }
        }
        else if (Vector128.IsHardwareAccelerated)
        {
            var search = Vector128.Create(hash);

            for (var i = 0; i < aligned; i += jump)
            {
                var value = Vector128.LoadUnsafe(ref d, (UIntPtr)i);
                if (Vector128.EqualsAny(value, search))
                {
                    var matches = Vector128.Equals(value, search).ExtractMostSignificantBits();

                    if (i + jump >= aligned)
                    {
                        // Undoing the multiplication done above to calculate aligned, to get the number of items.
                        var alignedCount = aligned / VectorsByBatch;
                        var toClear = alignedCount - count;

                        // This is the last in batch, masking is required to remove potential hits that are false positive
                        var hashesPerVector = VectorSize / sizeof(ushort);
                        var mask = (1U << hashesPerVector - toClear) - 1;
                        matches &= mask;
                    }

                    if (matches > 0)
                    {
                        var found = TryFind(i / VectorsByBatch, matches, key, preamble, out data);
                        if (found != NotFound)
                        {
                            return found;
                        }
                    }
                }
            }
        }
        else
        {
            ThrowNoVectorSupport();
        }

        data = default;
        return NotFound;

        [MethodImpl(MethodImplOptions.NoInlining)]
        void ThrowNoVectorSupport()
        {
            throw new NotSupportedException(
                $"This platform does not support {nameof(Vector256)} nor {nameof(Vector128)}");
        }
    }

    private int TryFind(int at, uint matches, in NibblePath key, byte preamble, out Span<byte> data)
    {
        var search = matches;

        Debug.Assert(search != 0);

        do
        {
            var index = BitOperations.TrailingZeroCount(search);

            // remove the match flag
            search ^= 1U << index;

            var i = index + at;

            ref var slot = ref GetSlotRef(i);

            // Preamble check is sufficient as IsDeleted is a special value of the preamble
            if ( /*slot.IsDeleted == false &&*/ slot.KeyPreamble == preamble)
            {
                var actual = GetSlotPayload(i);

                if (slot.HasKeyBytes)
                {
                    if (NibblePath.TryReadFrom(actual, key, out var leftover))
                    {
                        data = leftover;
                        return i;
                    }
                }
                else
                {
                    // The key is contained in the hash, all is equal and good to go!
                    data = actual;
                    return i;
                }
            }
        } while (search != 0);

        data = default;
        return NotFound;
    }

    /// <summary>
    /// Gets the payload pointed to by the given slot without the length prefix.
    /// </summary>
    [SkipLocalsInit]
    private Span<byte> GetSlotPayload(int index)
    {
        var addr = GetSlotRef(index).ItemAddress;

        // If this is the first, just slice of data
        if (index == 0)
            return _data[addr..];

        // Not the first, calculate on the basis of the address.
        var previousSlotAddress = GetSlotRef(index - 1).ItemAddress;
        var length = previousSlotAddress - addr;
        return _data.Slice(addr, length);
    }

    /// <summary>
    /// Exposes <see cref="Slot.PrepareKey"/> for tests only.
    /// </summary>
    public static ushort HashForTests(in NibblePath key) => Slot.PrepareKey(key, out _, out _);

    public static NibblePath UnPrepareKeyForTests(ushort hash, byte preamble, ReadOnlySpan<byte> input,
        Span<byte> workingSet, out ReadOnlySpan<byte> data) =>
        Slot.UnPrepareKey(hash, preamble, input, workingSet, out data);

    public static ushort PrepareKeyForTests(in NibblePath key, out byte preamble, out NibblePath trimmed) =>
        Slot.PrepareKey(key, out preamble, out trimmed);

    /// <summary>
    /// The slot is a size of <see cref="Size"/> bytes and represents non-hash part of the entry.
    /// The separation is done to make the search as vector aligned as possible.
    /// </summary>
    [StructLayout(LayoutKind.Sequential, Pack = sizeof(byte), Size = Size)]
    private struct Slot
    {
        /// <summary>
        /// The size of <see cref="Slot"/> with hash combined.
        /// </summary>
        public const int TotalSize = Size + sizeof(ushort);

        public const int Size = 2;

        /// <summary>
        /// The address currently allows to address 8kb of data 
        /// </summary>
        private const int MaxSize = 8 * 1024;

        /// <summary>
        /// The addressing requires 13 bits [0-12] to address whole page, leaving 3 bits for other purposes.
        /// </summary>
        private const ushort AddressMask = MaxSize - 1;

        /// <summary>
        /// The address of this item.
        /// </summary>
        public ushort ItemAddress
        {
            readonly get => (ushort)(Raw & AddressMask);
            set => Raw = (ushort)((Raw & ~AddressMask) | value);
        }

        /// <summary>
        /// Whether the given entry is deleted or not
        /// </summary>
        public bool IsDeleted => KeyPreamble == KeyPreambleDelete;

        /// <summary>
        /// Marks the slot as deleted
        /// </summary>
        public void MarkAsDeleted()
        {
            KeyPreamble = KeyPreambleDelete;
        }

        // Preamble uses all bits that AddressMask does not
        private const ushort KeyPreambleMask = unchecked((ushort)~AddressMask);
        private const ushort KeyPreambleShift = 13;

        // There are 3 bits left, where 0th bit is used to mark the oddity.
        // There are two more bits that allow encoding 4 values:
        // 00 - the keys is 3 or fewer nibbles long and length is encoded as one nibble
        // 01 - the key is 4 nibbles long
        // 10 - the key is 5 or more nibbles long has keys
        // 11 - not used currently

        // Shifts for lengths between 0 - 3. For odd, make it the highest
        private const int OddLengthShift = 3 * NibblePath.NibbleShift;
        private const int EvenLengthShift = 0;
        private const int Length1 = 1;
        private const int Length2 = 2;
        private const int Length3 = 3;

        private const byte KeyPreambleLength0 = 0b00;
        private const byte KeyPreambleLength3OrLess = 0b01;
        private const byte KeyPreambleLength4 = 0b10;
        private const byte KeyPreambleLength5OrMore = 0b11;
        private const byte KeyPreambleOddBit = 0b001; // The bit used for odd-starting paths.
        private const byte KeyPreambleDelete = KeyPreambleOddBit; // Empty cannot be odd, odd is used as deleted marker.
        // 0b110, 0b111 are not used

        public const byte KeyPreambleWithBytes = KeyPreambleLength5OrMore << KeyPreambleLengthShift;

        private const byte KeyPreambleLengthShift = 1;
        private const byte KeyPreambleMaxEncodedLength = 4;
        private const byte KeySlice = 2;

        private const int HashByteShift = 8;

        public bool HasAtLeastOneNibble => (KeyPreamble >> KeyPreambleLengthShift) > KeyPreambleLength0;

        public byte GetNibble0(ushort hash)
        {
            // Bitwise. Shift by 12, unless it's odd. If odd, shift by 8.
            return (byte)(0x0F & (hash >> (3 * NibblePath.NibbleShift -
                                           ((Raw >> KeyPreambleShift) & KeyPreambleOddBit) *
                                           NibblePath.NibbleShift)));
        }

        public byte KeyPreamble
        {
            readonly get => (byte)((Raw & KeyPreambleMask) >> KeyPreambleShift);
            set => Raw = (ushort)((Raw & ~KeyPreambleMask) | (value << KeyPreambleShift));
        }

        public bool HasKeyBytes => KeyPreamble >= KeyPreambleWithBytes;

        private ushort Raw;

        /// <summary>
        /// The memorized result of <see cref="PrepareKey"/> of this item.
        /// </summary>
        public readonly override string ToString() => $"{nameof(ItemAddress)}: {ItemAddress}";

        /// <summary>
        /// Prepares the key for the search. 
        /// </summary>
        public static ushort PrepareKey(in NibblePath key, out byte preamble, out NibblePath trimmed)
        {
            const int shift = NibblePath.NibbleShift;

            trimmed = NibblePath.Empty;
            var length = key.Length;
            preamble = (byte)(key.Oddity | (KeyPreambleLength3OrLess << KeyPreambleLengthShift));

            ref var b = ref key.UnsafeSpan;

            ushort hash = 0;

            switch ((length << 1) + key.Oddity)
            {
                case 0:
                case 1:
                    preamble = 0; // no oddity for empty, preamble is zero and so is hash
                    break;
                // length 1:
                case 2:
                    // even
                    hash = (ushort)(((b & 0xF0) << HashByteShift) + (Length1 << EvenLengthShift));
                    break;
                case 3:
                    // odd
                    hash = (ushort)(((b & 0x0F) << HashByteShift) + (Length1 << OddLengthShift));
                    break;
                // length 2:
                case 4:
                    // even
                    hash = (ushort)((b << HashByteShift) + (Length2 << EvenLengthShift));
                    break;
                case 5:
                    // odd
                    hash = (ushort)(((b & 0x0F) << HashByteShift) +
                                    (Unsafe.Add(ref b, 1) & 0xF0)
                                    + (Length2 << OddLengthShift)
                                    );
                    break;
                // length 3:
                case 6:
                    // even
                    hash = (ushort)((b << HashByteShift)
                                    + (Unsafe.Add(ref b, 1) & 0xF0)
                                    + (Length3 << EvenLengthShift));
                    break;
                case 7:
                    // odd
                    hash = (ushort)(
                        ((b & 0x0F) << HashByteShift)
                        + Unsafe.Add(ref b, 1)
                        + (Length3 << OddLengthShift));
                    break;
                // length 4:
                case 8:
                    // even
                    preamble = KeyPreambleLength4 << KeyPreambleLengthShift;
                    hash = (ushort)((b << HashByteShift) + Unsafe.Add(ref b, 1));
                    break;
                case 9:
                    // odd
                    preamble = KeyPreambleOddBit | (KeyPreambleLength4 << KeyPreambleLengthShift);
                    hash = (ushort)(
                        ((b & 0x0F) << HashByteShift) + // 0th 
                        Unsafe.Add(ref b, 1) + // 1th &2nd
                        ((Unsafe.Add(ref b, 2) & 0xF0) << HashByteShift) // 3rd, encoded as the highest
                    );
                    break;

                // beyond 4
                default:
                    preamble = (byte)(KeyPreambleWithBytes | key.Oddity);
                    trimmed = key.Slice(KeySlice + key.Oddity, length - KeyPreambleMaxEncodedLength);

                    Debug.Assert(trimmed.IsOdd == false, "Trimmed should be always even");

                    // The path is 4 nibbles or longer. The hash encoding is oddity dependent
                    // to make it easier to recover later.
                    if (key.IsOdd)
                    {
                        // Odd starting path _ABCxxxD will have its hash encoded as DABC

                        hash = (ushort)(
                            ((b & 0x0F) << HashByteShift) + // 0th 
                            Unsafe.Add(ref b, 1) + // 1th &2nd
                            (key.GetAt(length - 1) << (HashByteShift + shift)) // last, encoded as the highest
                        );
                    }
                    else
                    {
                        // even starting path ABxxxCD will have its hash encoded as ABDC
                        // this is done to efficiently decode it layer by slicing the hash.
                        hash = (ushort)(
                            (b << HashByteShift) + // 0th & 1st 
                            (key.GetAt(length - 2) << shift) + key.GetAt(length - 1)); // last but one & last    
                    }

                    break;
            }

            return hash;
        }

        [SkipLocalsInit]
        public static NibblePath UnPrepareKey(ushort hash, byte preamble, ReadOnlySpan<byte> input,
            Span<byte> workingSet, out ReadOnlySpan<byte> data)
        {
            var odd = preamble & KeyPreambleOddBit;
            var lengthBits = preamble >> KeyPreambleLengthShift;

            // Get directly reference, hash is big endian
            ref var b = ref MemoryMarshal.GetReference(workingSet);

            // Ensure low bits go low
            if (BitConverter.IsLittleEndian)
            {
                Unsafe.WriteUnaligned(ref b, BinaryPrimitives.ReverseEndianness(hash));
            }
            else
            {
                Unsafe.WriteUnaligned(ref b, hash);
            }

            data = input;


            switch (lengthBits)
            {
                case KeyPreambleLength0:
                    return NibblePath.Empty;

                case KeyPreambleLength3OrLess:
                    // 1, 2, or 3
                    // use bit-hash to get even or odd 
                    var length = GetLengthOf123(hash, odd);
                    return length == 0 ? default : NibblePath.FromKey(workingSet, odd, length);

                case KeyPreambleLength4:
                    if (odd == 0)
                    {
                        return NibblePath.FromKey(workingSet, 0, 4);
                    }

                    // The 4th nibble is written as 0th, write back and slice only
                    Unsafe.Add(ref b, 2) = b;
                    return NibblePath.FromKey(workingSet, 1, 4);
                default:
                    data = NibblePath.ReadFrom(input, out var trimmed);

                    Debug.Assert(trimmed.IsEmpty == false, "Trimmed cannot empty");

                    var result = NibblePath.FromKey(workingSet, odd, trimmed.Length + 4);

                    var raw = trimmed.RawSpan;

                    // Make branch free copy, if the path is odd, it will copy after byte 2, if even, after 1
                    raw.CopyTo(workingSet[(1 + odd)..]);

                    var at = 2 + trimmed.Length;

                    if (odd == KeyPreambleOddBit)
                    {
                        // only set the last for the odd
                        result.UnsafeSetAt(at + 1, (byte)((hash >> (HashByteShift + NibblePath.NibbleShift)) & 0x0F));
                    }
                    else
                    {
                        result.UnsafeSetAt(at, (byte)((hash & 0xF0) >> NibblePath.NibbleShift));
                        result.UnsafeSetAt(at + 1, (byte)(hash & 0x0F));
                    }

                    return result;
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static int GetLengthOf123(ushort hash, int odd)
        {
            var shift = OddLengthShift * odd;
            Debug.Assert(shift == (odd == 0 ? EvenLengthShift : OddLengthShift));
            var extract = NibblePath.NibbleMask << shift;
            return (hash & extract) >> shift;
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

        public readonly ushort TakenAfterOneMoreSlot => (ushort)(AlignToDoubleVectorSize(Low + Slot.TotalSize) + High);
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