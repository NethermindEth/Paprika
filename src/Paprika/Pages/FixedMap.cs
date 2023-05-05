﻿using System.Buffers;
using System.Buffers.Binary;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Paprika.Crypto;

namespace Paprika.Pages;

/// <summary>
/// Represents an in-page map, responsible for storing items and information related to them.
/// </summary>
/// <remarks>
/// The map is fixed in since as it's page dependent, hence the name.
/// It is a modified version of a slot array, that does not externalize slot indexes.
///
/// It keeps an internal map, now implemented with a not-the-best loop over slots.
/// With the use of key prefix, it should be small enough and fast enough for now.
/// </remarks>
public readonly ref struct FixedMap
{
    public const int MinSize = AllocationGranularity * 3;

    private const int AllocationGranularity = 8;
    private const int ItemLengthLength = 2;

    private readonly ref Header _header;
    private readonly Span<byte> _data;
    private readonly Span<Slot> _slots;
    private readonly Span<byte> _raw;

    public FixedMap(Span<byte> buffer)
    {
        _raw = buffer;
        _header = ref Unsafe.As<byte, Header>(ref _raw[0]);
        _data = buffer.Slice(Header.Size);
        _slots = MemoryMarshal.Cast<byte, Slot>(_data);
    }

    /// <summary>
    /// Represents the type of data stored in the map.
    /// </summary>
    public enum DataType : byte
    {
        /// <summary>
        /// [key, 0]-> account (balance, nonce)
        /// </summary>
        Account = 0,

        /// <summary>
        /// [key, 1]-> codeHash
        /// </summary>
        CodeHash = 1,

        /// <summary>
        /// [key, 2]-> storageRootHash
        /// </summary>
        StorageRootHash = 2,

        /// <summary>
        /// [key, 3]-> [index][value],
        /// add to the key and use first 32 bytes of data as key
        /// </summary>
        StorageCell = 3,

        /// <summary>
        /// [key, 4]-> the DbAddress of the root page of the storage trie,
        /// </summary>
        StorageTreeRootPageAddress = 4,

        /// <summary>
        /// [storageCellIndex, 5]-> the StorageCell index, without the prefix of the account
        /// </summary>
        StorageTreeStorageCell = 5,

        // Unused3 = 6,
        // Unused4 = 7
    }

    /// <summary>
    /// Represents the key of the map.
    /// </summary>
    public readonly ref struct Key
    {
        public readonly NibblePath Path;
        public readonly DataType Type;
        public readonly ReadOnlySpan<byte> AdditionalKey;

        private Key(NibblePath path, DataType type, ReadOnlySpan<byte> additionalKey)
        {
            Path = path;
            Type = type;
            AdditionalKey = additionalKey;
        }

        /// <summary>
        /// To be used only by FixedMap internally. Builds the raw key
        /// </summary>
        public static Key Raw(NibblePath path, DataType type) =>
            new(path, type, ReadOnlySpan<byte>.Empty);

        /// <summary>
        /// Builds the key for <see cref="DataType.Account"/>.
        /// </summary>
        public static Key Account(NibblePath path) => new(path, DataType.Account, ReadOnlySpan<byte>.Empty);

        /// <summary>
        /// Builds the key for <see cref="DataType.CodeHash"/>.
        /// </summary>
        public static Key CodeHash(NibblePath path) => new(path, DataType.CodeHash, ReadOnlySpan<byte>.Empty);

        /// <summary>
        /// Builds the key for <see cref="DataType.StorageRootHash"/>.
        /// </summary>
        public static Key StorageRootHash(NibblePath path) =>
            new(path, DataType.StorageRootHash, ReadOnlySpan<byte>.Empty);

        /// <summary>
        /// Builds the key for <see cref="DataType.StorageCell"/>.
        /// </summary>
        /// <remarks>
        /// <paramref name="keccak"/> must be passed by ref, otherwise it will blow up the span!
        /// </remarks>
        public static Key StorageCell(NibblePath path, in Keccak keccak) =>
            new(path, DataType.StorageCell, keccak.Span);

        /// <summary>
        /// Builds the key for <see cref="DataType.StorageCell"/>.
        /// </summary>
        /// <remarks>
        /// <paramref name="keccak"/> must be passed by ref, otherwise it will blow up the span!
        /// </remarks>
        public static Key StorageCell(NibblePath path, ReadOnlySpan<byte> keccak) =>
            new(path, DataType.StorageCell, keccak);

        public Key SliceFrom(int nibbles) => new(Path.SliceFrom(nibbles), Type, AdditionalKey);

        public override string ToString()
        {
            return $"{nameof(Path)}: {Path.ToString()}, " +
                   $"{nameof(Type)}: {Type}, " +
                   $"{nameof(AdditionalKey)}: {AdditionalKey.ToHexString(false)}";
        }
    }

    public bool TrySet(in Key key, ReadOnlySpan<byte> data)
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

        var hash = Slot.ExtractPrefix(key.Path);
        var encodedKey = key.Path.WriteTo(stackalloc byte[key.Path.MaxByteLength]);

        // does not exist yet, calculate total memory needed
        var total = GetTotalSpaceRequired(encodedKey, key.AdditionalKey, data);

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
        slot.Prefix = hash;
        slot.ItemAddress = (ushort)(_data.Length - _header.High - total);
        slot.Type = key.Type;

        // write item: length, key, additionalKey, data
        var dest = _data.Slice(slot.ItemAddress, total);

        WriteEntryLength(dest, (ushort)(total - ItemLengthLength));

        encodedKey.CopyTo(dest.Slice(ItemLengthLength));
        key.AdditionalKey.CopyTo(dest.Slice(ItemLengthLength + encodedKey.Length));
        data.CopyTo(dest.Slice(ItemLengthLength + encodedKey.Length + key.AdditionalKey.Length));

        // commit low and high
        _header.Low += Slot.Size;
        _header.High += total;
        return true;
    }

    private int To => _header.Low / Slot.Size;

    public NibbleEnumerator EnumerateNibble(byte nibble) => new(this, nibble);

    public ref struct NibbleEnumerator
    {
        /// <summary>The map being enumerated.</summary>
        private readonly FixedMap _map;

        /// <summary>
        /// The nibble being enumerated.
        /// </summary>
        private readonly byte _nibble;

        /// <summary>The next index to yield.</summary>
        private int _index;

        internal NibbleEnumerator(FixedMap map, byte nibble)
        {
            _map = map;
            _nibble = nibble;
            _index = -1;
        }

        /// <summary>Advances the enumerator to the next element of the span.</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool MoveNext()
        {
            int index = _index + 1;
            var to = _map.To;

            while (index < to &&
                   (_map._slots[index].IsDeleted ||
                    _map._slots[index].FirstNibbleOfPrefix != _nibble))
            {
                index += 1;
            }

            if (index < to)
            {
                _index = index;
                return true;
            }

            return false;
        }

        public Item Current
        {
            get
            {
                var slot = _map._slots[_index];
                var span = _map.GetSlotPayload(slot);
                var data = NibblePath.ReadFrom(span, out var path);

                if (slot.Type == DataType.StorageCell)
                {
                    const int size = Keccak.Size;
                    var additionalKey = data.Slice(0, size);
                    return new Item(Key.StorageCell(path, additionalKey), data.Slice(size));
                }

                return new Item(Key.Raw(path, slot.Type), data);
            }
        }

        public readonly ref struct Item
        {
            public Key Key { get; }
            public ReadOnlySpan<byte> RawData { get; }

            public Item(Key key, ReadOnlySpan<byte> rawData)
            {
                Key = key;
                RawData = rawData;
            }
        }

        // a shortcut to not allocate, just copy the enumerator
        public NibbleEnumerator GetEnumerator() => this;
    }

    /// <summary>
    /// Gets the nibble representing the biggest bucket.
    /// </summary>
    /// <returns></returns>
    public byte GetBiggestNibbleBucket()
    {
        const int bucketCount = 16;

        Span<ushort> buckets = stackalloc ushort[bucketCount];

        var to = _header.Low / Slot.Size;
        for (var i = 0; i < to; i++)
        {
            ref readonly var slot = ref _slots[i];
            if (slot.IsDeleted == false)
            {
                var index = slot.FirstNibbleOfPrefix % bucketCount;
                buckets[index]++;
            }
        }

        var maxI = 0;

        for (int i = 1; i < bucketCount; i++)
        {
            var currentCount = buckets[i];
            var maxCount = buckets[maxI];
            if (currentCount > maxCount)
            {
                maxI = i;
            }
        }

        return (byte)maxI;
    }

    private static ushort GetTotalSpaceRequired(ReadOnlySpan<byte> key, ReadOnlySpan<byte> additionalKey,
        ReadOnlySpan<byte> data)
    {
        return (ushort)(key.Length + data.Length + additionalKey.Length + ItemLengthLength);
    }

    /// <summary>
    /// Warning! This does not set any tombstone so the reader won't be informed about a delete,
    /// just will miss the value.
    /// </summary>
    public bool Delete(in Key key)
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
        _slots[index].IsDeleted = true;
        _header.Deleted++;

        // always try to compact after delete
        CollectTombstones();
    }

    private void Deframent()
    {
        // s as data were fitting before, the will fit after so all the checks can be skipped
        var size = _raw.Length;
        var array = ArrayPool<byte>.Shared.Rent(size);
        var span = array.AsSpan(0, size);

        span.Clear();
        var copy = new FixedMap(span);
        var count = _header.Low / Slot.Size;

        for (int i = 0; i < count; i++)
        {
            var copyFrom = _slots[i];
            if (copyFrom.IsDeleted == false)
            {
                var source = _data.Slice(copyFrom.ItemAddress);
                var length = ReadDataLength(source);
                var fromSpan = source.Slice(0, length + ItemLengthLength);

                ref var copyTo = ref copy._slots[copy._header.Low / Slot.Size];

                // copy raw, no decoding
                var high = (ushort)(copy._data.Length - copy._header.High - fromSpan.Length);
                fromSpan.CopyTo(copy._data.Slice(high));

                copyTo.Prefix = copyFrom.Prefix;
                copyTo.ItemAddress = high;
                copyTo.Type = copyFrom.Type;

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
        var index = To - 1;

        while (index >= 0 && _slots[index].IsDeleted)
        {
            // undo writing low
            _header.Low -= Slot.Size;

            // undo writing high
            var slice = _data.Slice(_slots[index].ItemAddress);
            var total = ReadDataLength(slice) + ItemLengthLength;
            _header.High = (ushort)(_header.High - total);

            // cleanup
            _slots[index] = default;
            _header.Deleted--;

            // move back by one to see if it's deleted as well
            index--;
        }
    }

    [OptimizationOpportunity(OptimizationType.DiskSpace,
        "Use var-length encoding for prefix as it will be usually small. This may save 1 byte per entry.")]
    private static void WriteEntryLength(Span<byte> dest, ushort dataRequiredWithNoLength) =>
        BinaryPrimitives.WriteUInt16LittleEndian(dest, dataRequiredWithNoLength);

    [OptimizationOpportunity(OptimizationType.DiskSpace,
        "Use var-length encoding for prefix as it will be usually small. This may save 1 byte per entry.")]
    private static ushort ReadDataLength(Span<byte> source) => BinaryPrimitives.ReadUInt16LittleEndian(source);

    public bool TryGet(in Key key, out ReadOnlySpan<byte> data)
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
        "Scanning through slots could be done with MemoryMarshal.Cast<Slot, ushort> and IndexOf? " +
        "When index is odd, this means prefix is hit.")]
    [OptimizationOpportunity(OptimizationType.CPU,
        "key.Write to might be called twice, here and in TrySet")]
    private bool TryGetImpl(in Key key, out Span<byte> data, out int slotIndex)
    {
        var hash = Slot.ExtractPrefix(key.Path);
        var encodedKey = key.Path.WriteTo(stackalloc byte[key.Path.MaxByteLength]);

        // optimization opportunity
        // var cast = MemoryMarshal.Cast<Slot, ushort>(_slots.Slice(_header.Low));
        // cast.IndexOf(hash);

        var to = _header.Low / Slot.Size;
        for (int i = 0; i < to; i++)
        {
            ref readonly var slot = ref _slots[i];
            if (slot.IsDeleted == false &&
                slot.Prefix == hash &&
                slot.Type == key.Type)
            {
                var actual = GetSlotPayload(slot);

                // The StartsWith check assumes that all the keys have the same length.
                if (actual.StartsWith(encodedKey))
                {
                    if (key.AdditionalKey.IsEmpty)
                    {
                        // no additional key, just assert encoded
                        data = actual.Slice(encodedKey.Length);
                        slotIndex = i;
                        return true;
                    }

                    // there's the additional key, assert it
                    // do it by slicing off first the encoded and then check the additional
                    if (actual.Slice(encodedKey.Length).StartsWith(key.AdditionalKey))
                    {
                        data = actual.Slice(encodedKey.Length + key.AdditionalKey.Length);
                        slotIndex = i;
                        return true;
                    }
                }
            }
        }

        data = default;
        slotIndex = default;
        return false;
    }

    /// <summary>Gets an enumerator for this span.</summary>
    public Enumerator GetEnumerator() => new(this);

    /// <summary>Enumerates the elements of a <see cref="ReadOnlySpan{T}"/>.</summary>
    public ref struct Enumerator
    {
        /// <summary>The map being enumerated.</summary>
        private readonly FixedMap _map;

        /// <summary>The slot index to yield.</summary>
        private int _index;

        /// <summary>Initialize the enumerator.</summary>
        /// <param name="map">The map to enumerate.</param>
        internal Enumerator(FixedMap map)
        {
            _map = map;
            _index = -1;
        }

        /// <summary>Advances the enumerator to the next element of the span.</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool MoveNext()
        {
            var to = _map.To;

            var index = _index + 1;

            // skip all deleted
            while (_map._slots[index].IsDeleted && index < to)
            {
                index += 1;
            }

            if (index < to)
            {
                _index = index;
                return true;
            }

            return false;
        }

        /// <summary>Gets the element at the current position of the enumerator.</summary>
        public readonly Item Current
        {
            get
            {
                var payload = _map.GetSlotPayload(_map._slots[_index]);
                var data = NibblePath.ReadFrom(payload, out var path);
                return new Item(path, data);
            }
        }

        public readonly ref struct Item
        {
            public NibblePath Path { get; }
            public ReadOnlySpan<byte> Data { get; }

            public Item(NibblePath path, ReadOnlySpan<byte> data)
            {
                Path = path;
                Data = data;
            }
        }
    }

    /// <summary>
    /// Gets the payload pointed to by the given slot without the length prefix.
    /// </summary>
    private Span<byte> GetSlotPayload(Slot slot)
    {
        var slice = _data.Slice(slot.ItemAddress);
        var dataLength = ReadDataLength(slice);
        return slice.Slice(ItemLengthLength, dataLength);
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

        // IsDeleted
        private const int DeletedShift = 15;
        private const ushort DeletedBit = 1 << DeletedShift;

        /// <summary>
        /// Whether it's deleted.
        /// </summary>
        public bool IsDeleted
        {
            get => (Raw & DeletedBit) == DeletedBit;
            set => Raw = (ushort)((Raw & ~DeletedBit) | (value ? DeletedBit : 0));
        }

        private const int DataTypeShift = 12;
        private const ushort DataTypeMask = unchecked((ushort)(111 << DataTypeShift));

        /// <summary>
        /// The data type contained in this slot.
        /// </summary>
        public DataType Type
        {
            get => (DataType)((Raw & DataTypeMask) >> DataTypeShift);
            set => Raw = (ushort)((Raw & ~DataTypeMask) | (ushort)((byte)value << DataTypeShift));
        }

        [FieldOffset(0)] private ushort Raw;

        /// <summary>
        /// The memorized result of <see cref="ExtractPrefix"/> of this item.
        /// </summary>
        [FieldOffset(2)] public ushort Prefix;

        /// <summary>
        /// Builds the hash for the key.
        /// </summary>
        [OptimizationOpportunity(OptimizationType.DiskSpace,
            "NibblePath provides no support for concat so no easy way to reintroduce the prefix here. " +
            "If it provided one, 2 bytes to save per key")]
        public static ushort ExtractPrefix(NibblePath key)
        {
            var prefix = (key.GetAt(0) << NibblePath.NibbleShift * 0) +
                         (key.GetAt(1) << NibblePath.NibbleShift * 1) +
                         (key.GetAt(2) << NibblePath.NibbleShift * 2) +
                         (key.GetAt(3) << NibblePath.NibbleShift * 3);

            // optimization           
            //rest = key.SliceFrom(4);
            return (ushort)prefix;
        }

        public byte FirstNibbleOfPrefix => (byte)(Prefix & 0x0F);

        public override string ToString()
        {
            return
                $"{nameof(Type)}: {Type}, {nameof(Prefix)}: {Prefix}, {nameof(ItemAddress)}: {ItemAddress}, {nameof(IsDeleted)}: {IsDeleted}";
        }
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