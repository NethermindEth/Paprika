﻿using System.Buffers.Binary;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace Paprika.Pages;

/// <summary>
/// Represents an in-page buffer, responsible for storing items and information related to them.
/// </summary>
public readonly ref struct PageBuffer
{
    public const int AllocationGranularity = 8;
    public const int MixSize = AllocationGranularity * 3;

    private readonly ref Header _header;
    private readonly Span<byte> _data;
    private readonly Span<ItemId> _ids;

    private const int Null = 0;

    public PageBuffer(Span<byte> buffer)
    {
        _header = ref Unsafe.As<byte, Header>(ref buffer[0]);
        _data = buffer.Slice(Header.Size);
        _ids = MemoryMarshal.Cast<byte, ItemId>(_data);
    }

    public bool TrySet(Span<byte> key, Span<byte> data, ref Index<ushort> prev)
    {
        var prefix = ItemId.ExtractKeyBytes(key);

        if (prev.IsNull)
        {
            // try search first and replace in situ. If possible,
            // if the size is different, mark as deleted and default to the next
        }

        // does not exist yet, calculate
        var dataRequiredWithNoLength =
            (ushort)(key.Length + data.Length - ItemId.KeyExtractedBytes);
        var dataRequired = (ushort)(dataRequiredWithNoLength + ItemId.ItemLengthLength);

        if (_header.Taken + dataRequired + ItemId.Size > _data.Length)
        {
            return false;
        }

        var at = _header.Low;
        ref var id = ref _ids[at / ItemId.Size];

        // write ItemId
        id.ItemPrefix = prefix;
        id.NextAddress = prev.Raw;
        id.ItemAddress = (ushort)(_data.Length - _header.Low - dataRequired);

        // write item, first length, then key, then data
        var dest = _data.Slice(id.ItemAddress, dataRequired);
        WriteDataLength(dest, dataRequiredWithNoLength);

        key.Slice(ItemId.KeyExtractedBytes).CopyTo(dest.Slice(ItemId.ItemLengthLength));
        data.CopyTo(dest.Slice(ItemId.ItemLengthLength + key.Length - ItemId.KeyExtractedBytes));

        // commit low and high
        prev = Index<ushort>.FromIndex(_header.Low);
        _header.Low += ItemId.Size;
        _header.High += dataRequired;
        return true;
    }

    private static void WriteDataLength(Span<byte> dest, ushort dataRequiredWithNoLength) =>
        BinaryPrimitives.WriteUInt16LittleEndian(dest, dataRequiredWithNoLength);

    private static ushort ReadDataLength(Span<byte> source) => BinaryPrimitives.ReadUInt16LittleEndian(source);

    public bool TryGet(Span<byte> key, out Span<byte> data, Index<ushort> start)
    {
        var prefix = ItemId.ExtractKeyBytes(key);

        var index = start;
        while (index.IsNull == false)
        {
            ref readonly var id = ref _ids[index.Value];

            // compare only not deleted and with the same prefix
            if (id.IsDeleted == false && id.ItemPrefix == prefix)
            {
                var slice = _data.Slice(id.ItemAddress);
                var dataLength = ReadDataLength(slice);
                var actual = slice.Slice(2, dataLength);

                // this check assumes that all the keys have the same length.
                // Otherwise, prefix length would be required
                var keyLeftover = key.Slice(ItemId.KeyExtractedBytes);
                if (actual.StartsWith(keyLeftover))
                {
                    data = actual.Slice(keyLeftover.Length);
                    return true;
                }
            }

            index = Index<ushort>.FromRaw(id.NextAddress);
        }

        data = default;
        return false;
    }

    [StructLayout(LayoutKind.Explicit, Size = Size)]
    private struct ItemId
    {
        public const int Size = 8;
        public const int KeyExtractedBytes = 4;
        public const int ItemLengthLength = 2;

        private const int BitsForPageAddress = 12;
        private const uint InPageAddressMask = Page.PageSize - 1;
        private const uint Bit = 1;

        private const int NextShift = BitsForPageAddress * 0;
        private const int ItemShift = BitsForPageAddress * 1;
        private const int DeletedShift = BitsForPageAddress * 2; // 24

        /// <summary>
        /// The next chained item.
        /// </summary>
        public ushort NextAddress
        {
            get => (ushort)((Raw >> NextShift) & InPageAddressMask);
            set => Raw = Raw & ~(uint)(InPageAddressMask << NextShift) | ((uint)value << NextShift);
        }

        /// <summary>
        /// The address of this item.
        /// </summary>
        public ushort ItemAddress
        {
            get => (ushort)((Raw >> ItemShift) & InPageAddressMask);
            set => Raw = Raw & ~(InPageAddressMask << ItemShift) | ((uint)value << ItemShift);
        }

        public bool IsDeleted
        {
            get => ((Raw >> DeletedShift) & Bit) == Bit;
            set => Raw = Raw & ~(Bit << DeletedShift) | ((value ? Bit : 0) << ItemShift);
        }

        [FieldOffset(0)] private uint Raw;

        /// <summary>
        /// First 4 bytes extracted from the item for fast comparisons.
        /// </summary>
        [FieldOffset(4)] public uint ItemPrefix;

        public static uint ExtractKeyBytes(Span<byte> key) => BinaryPrimitives.ReadUInt32LittleEndian(key);
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