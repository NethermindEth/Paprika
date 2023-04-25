using System.Buffers.Binary;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace Paprika.Pages;

/// <summary>
/// Represents an in-page buffer, responsible for storing items and information related to them.
/// </summary>
/// <remarks>
/// The page buffer is a modified version of a slot array, that does not extrenalize slot indexes.
/// It keeps an internal map, now implemented with a not-the-best loop over slots. With the use of hash,
/// it should be small enough and fast enough for now.
/// </remarks>
public readonly ref struct PageBuffer
{
    public const int AllocationGranularity = 8;
    public const int MixSize = AllocationGranularity * 3;

    private const int ItemLengthLength = 2;

    private readonly ref Header _header;
    private readonly Span<byte> _data;
    private readonly Span<Slot> _slots;

    public enum SetOptions
    {
        /// <summary>
        /// Informs that the value encodes length in a way,
        /// that a bigger bucket can be used for the value. 
        /// </summary>
        ValueEncodesLength,

        None
    }

    public PageBuffer(Span<byte> buffer)
    {
        _header = ref Unsafe.As<byte, Header>(ref buffer[0]);
        _data = buffer.Slice(Header.Size);
        _slots = MemoryMarshal.Cast<byte, Slot>(_data);
    }

    public bool TrySet(ReadOnlySpan<byte> key, ReadOnlySpan<byte> data, SetOptions options = SetOptions.None)
    {
        var hash = Slot.HashKey(key);

        // TODO: implement search and in-situ replacement

        // does not exist yet, calculate
        var dataRequiredWithNoLength = (ushort)(key.Length + data.Length);
        var dataRequired = (ushort)(dataRequiredWithNoLength + Slot.ItemLengthLength);

        if (_header.Taken + dataRequired + Slot.Size > _data.Length)
        {
            return false;
        }

        var at = _header.Low;
        ref var id = ref _slots[at / Slot.Size];

        // write slot
        id.Hash = hash;
        id.ItemAddress = (ushort)(_data.Length - _header.Low - dataRequired);
        id.IsDeleted = false;

        // write item, first length, then key, then data
        var dest = _data.Slice(id.ItemAddress, dataRequired);
        WriteDataLength(dest, dataRequiredWithNoLength);

        key.CopyTo(dest.Slice(Slot.ItemLengthLength));
        data.CopyTo(dest.Slice(Slot.ItemLengthLength + key.Length));

        // commit low and high
        _header.Low += Slot.Size;
        _header.High += dataRequired;
        return true;
    }

    public bool Delete(ReadOnlySpan<byte> key)
    {
        if (TryGetImpl(key, out _, out var index))
        {
            // mark as deleted first
            _slots[index].IsDeleted = true;

            // check if it's the last one and free space if possible
            var lastWrittenIndex = _header.Low / Slot.Size - 1;

            if (index == lastWrittenIndex)
            {
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

                    // move back by one to see if it's deleted as well
                    index--;
                }

                return true;
            }

            return true;
        }

        return false;
    }

    private static void WriteDataLength(Span<byte> dest, ushort dataRequiredWithNoLength) =>
        BinaryPrimitives.WriteUInt16LittleEndian(dest, dataRequiredWithNoLength);

    private static ushort ReadDataLength(Span<byte> source) => BinaryPrimitives.ReadUInt16LittleEndian(source);

    public bool TryGet(ReadOnlySpan<byte> key, out ReadOnlySpan<byte> data)
    {
        if (TryGetImpl(key, out var span, out _))
        {
            data = span;
            return true;
        }

        data = default;
        return false;
    }

    private bool TryGetImpl(ReadOnlySpan<byte> key, out Span<byte> data, out int slotIndex)
    {
        var hash = Slot.HashKey(key);

        // TODO: replace with
        // var cast = MemoryMarshal.Cast<Slot, ushort>(_slots.Slice(_header.Low));
        // cast.IndexOf(hash);

        var to = _header.Low / Slot.Size;
        for (int i = 0; i < to; i++)
        {
            ref readonly var slot = ref _slots[i];
            if (slot.IsDeleted == false &&
                slot.Hash == hash)
            {
                var slice = _data.Slice(slot.ItemAddress);
                var dataLength = ReadDataLength(slice);
                var actual = slice.Slice(2, dataLength);

                // The StartsWith check assumes that all the keys have the same length.
                if (actual.StartsWith(key))
                {
                    data = actual.Slice(key.Length);
                    slotIndex = i;
                    return true;
                }
            }
        }

        data = default;
        slotIndex = default;
        return false;
    }

    [StructLayout(LayoutKind.Explicit, Size = Size)]
    private struct Slot
    {
        public const int Size = 8;
        public const int ItemLengthLength = 2;

        // ItemAddress
        private const ushort AddressMask = Page.PageSize - 1;

        // IsDeleted
        private const int DeletedShift = 15;
        private const ushort DeletedBit = 1 << DeletedShift;

        /// <summary>
        /// The address of this item.
        /// </summary>
        public ushort ItemAddress
        {
            get => (ushort)(Raw & AddressMask);
            set => Raw = (ushort)((Raw & ~AddressMask) | value);
        }

        /// <summary>
        /// Whether it's deleted
        /// </summary>
        public bool IsDeleted
        {
            get => (Raw & DeletedBit) == DeletedBit;
            set => Raw = (ushort)((Raw & ~DeletedBit) | (value ? DeletedBit : 0));
        }

        [FieldOffset(0)] private ushort Raw;

        /// <summary>
        /// The memorized result of <see cref="HashKey"/> of this item.
        /// </summary>
        [FieldOffset(2)] public ushort Hash;

        /// <summary>
        /// Builds the hash for the key.
        /// </summary>
        public static ushort HashKey(ReadOnlySpan<byte> key)
        {
            var prefix = BinaryPrimitives.ReadUInt32LittleEndian(key);
            return unchecked((ushort)(prefix ^ (prefix >> 16)));
        }
    }

    [StructLayout(LayoutKind.Explicit, Size = Size)]
    private struct Header
    {
        public const int Size = 8;

        /// <summary>
        /// Represents the distance from the start.t
        /// </summary>
        [FieldOffset(0)] public ushort Low;

        /// <summary>
        /// Represents the distance from the end
        /// </summary>
        [FieldOffset(2)] public ushort High;

        /// <summary>
        /// How much was deleted in this page
        /// </summary>
        [FieldOffset(4)] public ushort Deleted;

        public ushort Taken => (ushort)(Low + High);
    }
}