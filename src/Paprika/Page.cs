﻿using System.Buffers.Binary;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using static System.Runtime.CompilerServices.Unsafe;

namespace Paprika;

/// <summary>
/// Struct representing data oriented page types.
/// Two separate types are used: Value and Jump page.
/// Jump pages consist only of jumps according to a part of <see cref="NibblePath"/>.
/// Value pages have buckets + skip list for storing values.
/// </summary>
public readonly unsafe struct Page
{
    public const int PageCount = 0x0100_0000; // 64GB addressable

    public const int PageSize = 4 * 1024;

    private const int MaxFanOutLevel = 1;

    private readonly void* _ptr;

    public Page(byte* ptr)
    {
        _ptr = ptr;
    }

    public TPage* As<TPage>() where TPage : unmanaged => (TPage*)_ptr;

    public UIntPtr Raw => new(_ptr);

    public void Clear() => new Span<byte>(_ptr, PageSize).Clear();

    private void ClearToWritable()
    {
        Clear();
        Flags = PageFlags.Writable;
    }

    public void MarkWritable()
    {
        Flags = PageFlags.Writable;
    }

    public void CopyTo(in Page page) => new Span<byte>(_ptr, PageSize).CopyTo(new Span<byte>(page._ptr, PageSize));

    public Page Set(in NibblePath key, in ReadOnlySpan<byte> value, int depth, IInternalTransaction tx)
    {
        var page = this;

        if (!page.IsWritable)
        {
            page = GetWritableCopy(tx, in this, out _);
        }

        PageFlags type = page.Flags & PageFlags.TypeMask;

        if (type == PageFlags.None)
        {
            // type not set yet, define it on the basis of the fan out level
            page.Flags = type = depth > MaxFanOutLevel ? PageFlags.ValuePage : PageFlags.JumpPage;

            // this is a newly allocated empty page, mark as writable
            page.Flags |= PageFlags.Writable;
        }

        if (type == PageFlags.JumpPage)
        {
            return JumpPage.Set(page, key, value, depth, tx);
        }

        if (type == PageFlags.ValuePage)
        {
            return ValuePage.Set(page, key, value, tx);
        }

        return page;
    }

    public void ClearWritable()
    {
        // TODO: clear writable flags first
    }

    private static Page GetWritableCopy(IInternalTransaction tx, in Page page, out int addr)
    {
        var allocated = tx.GetNewDirtyPage(out addr);
        page.CopyTo(allocated);
        tx.Abandon(page);
        allocated.Flags |= PageFlags.Writable;
        return allocated;
    }

    public bool TryGet(NibblePath key, out ReadOnlySpan<byte> value, int depth, IInternalTransaction manager)
    {
        PageFlags type = Flags & PageFlags.TypeMask;

        if (type == PageFlags.JumpPage)
        {
            return JumpPage.TryGet(this, key, out value, depth, manager);
        }

        if (type == PageFlags.ValuePage)
        {
            return ValuePage.TryGet(this, key, out value, manager);
        }

        value = default;
        return false;
    }

    private PageFlags Flags
    {
        get => AsRef<PageFlags>(_ptr);
        set => AsRef<PageFlags>(_ptr) = value;
    }

    private bool IsWritable => (Flags & PageFlags.Writable) == PageFlags.Writable;

    /// <summary>
    /// Represents the page type.
    /// Values of this do not overlap with AddressMask, allowing
    /// </summary>
    [Flags]
    enum PageFlags : byte
    {
        None = 0,

        /// <summary>
        /// The jump page consists of 1024 address that navigate to another page with values.
        /// 1024 is equal to 16 * 16 * 4 giving 2.5 nibbles of addressing.
        /// </summary>
        JumpPage = 0b0100_0000,

        /// <summary>
        /// The value page consists of 16 buckets with address, that navigate either internally to the page or to
        /// another ValuePage. ValuePages create a link list. Two pages should be enough to map almost all values.
        /// </summary>
        ValuePage = 0b1000_0000,

        /// <summary>
        /// Marks the page as writable within the given transaction.
        /// </summary>
        Writable = 0b0010_0000,

        TypeMask = JumpPage | ValuePage,
    }

    static class JumpPage
    {
        private const int Null = 0;
        private const int AddressSize = 4;

        /// <summary>
        /// Tries to find the <paramref name="path"/> in the given page.
        /// </summary>
        public static bool TryGet(in Page page, in NibblePath path, out ReadOnlySpan<byte> value, int depth,
            IInternalTransaction manager)
        {
            var addr = ReadAddr(page, path, depth);

            if (addr == Null)
            {
                value = default;
                return false;
            }

            var child = manager.GetAt(addr);
            return child.TryGet(SlicePath(path, depth), out value, depth + 1, manager);
        }

        [DebuggerStepThrough]
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static NibblePath SlicePath(NibblePath path, int depth) => path.SliceFrom(2 + OneOnOdd(depth));

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static void* GetAddrPtr(in Page page, NibblePath path, int depth)
        {
            // 2.5 nibble 
            var addr = OneOnOdd(depth) == 1 // odd means start at half
                ? ((path.GetAt(0) & 0x03) << 8) | (path.GetAt(1) << 4) | path.GetAt(2)
                : (path.GetAt(0) << 6) | (path.GetAt(1) << 2) | (path.GetAt(2) >> 4);

            return Add<byte>(page._ptr, addr * AddressSize);
        }

        private static int ReadAddr(in Page page, NibblePath path, int depth)
        {
            var addr = Read<int>(GetAddrPtr(page, path, depth));
            if (!BitConverter.IsLittleEndian)
            {
                addr = BinaryPrimitives.ReverseEndianness(addr);
            }

            return addr >> 8;
        }

        private static void WriteAddr(in Page page, NibblePath path, int depth, int actual)
        {
            var addrPtr = GetAddrPtr(page, path, depth);
            // the page is written to so it must be writable
            var addr = (actual << 8) | (int)(PageFlags.JumpPage | PageFlags.Writable);
            if (!BitConverter.IsLittleEndian)
            {
                addr = BinaryPrimitives.ReverseEndianness(addr);
            }

            Write(addrPtr, addr);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static int OneOnOdd(int depth) => depth & 1;

        public static Page Set(in Page page, in NibblePath key, in ReadOnlySpan<byte> value, int depth,
            IInternalTransaction tx)
        {
            var addr = ReadAddr(page, key, depth);

            if (addr == Null)
            {
                // does not exist, get and set
                var allocated = tx.GetNewDirtyPage(out addr);
                allocated.ClearToWritable();
            }

            var child = tx.GetAt(addr);

            // remember, the child might have changed to another ref
            var actual = tx.GetAddress(child.Set(SlicePath(key, depth), value, depth + 1, tx));

            WriteAddr(page, key, depth, actual);

            return page;
        }
    }

    static class ValuePage
    {
        [StructLayout(LayoutKind.Explicit, Size = Size, Pack = 1)]
        private struct Header
        {
            public const int Size = NibbleStart + NibbleCount * AddressSize;

            private const int NibbleStart = 8;

            /// <summary>
            /// <see cref="ValuePage.GetKeyIndex"/> to understand the mappning.
            /// </summary>
            private const int NibbleCount = 16 * 4;

            [FieldOffset(2)] public ushort MemoryUsed;
            [FieldOffset(4)] public int OverflowTo;
            [FieldOffset(NibbleStart)] public fixed ushort Nibble[NibbleCount];
        }

        private const int AddressSize = 2;
        private const int AvailableMemoryInPage = PageSize - Header.Size;
        private const int LengthOfLenght = 1;
        private const int Null = 0;
        private const ushort NullAddr = 0;
        private const int KeySlice = 1;

        public static Page Set(in Page page, in NibblePath key, in ReadOnlySpan<byte> value, IInternalTransaction tx)
        {
            if (TryWrite(page, key, value))
            {
                return page;
            }

            // slow path, overflow
            ref var header = ref AsRef<Header>(page._ptr);
            if (header.OverflowTo != Null)
            {
                var overflow = tx.GetAt(header.OverflowTo);
                if (!overflow.IsWritable)
                {
                    overflow = GetWritableCopy(tx, overflow, out header.OverflowTo);
                }

                Set(in overflow, key, value, tx);
                return page;
            }
            else
            {
                var allocated = tx.GetNewDirtyPage(out header.OverflowTo);
                allocated.ClearToWritable();

                Set(in allocated, key, value, tx);
                return page;
            }
        }

        private static bool TryWrite(Page page, NibblePath key, ReadOnlySpan<byte> value)
        {
            var header = (Header*)page._ptr;

            var index = GetKeyIndex(key);
            var addr = header->Nibble[index];

            key = key.SliceFrom(KeySlice);

            var neededMemory = (ushort)(key.RawByteLength + AddressSize + LengthOfLenght + value.Length);

            var available = AvailableMemoryInPage - header->MemoryUsed;
            if (available >= neededMemory)
            {
                var destination = new Span<byte>(Add<byte>(page._ptr, Header.Size + header->MemoryUsed), neededMemory);

                var leftover = key.WriteTo(destination);
                WriteUnaligned(ref leftover[0], addr);
                leftover[AddressSize] = (byte)value.Length;
                value.CopyTo(leftover.Slice(AddressSize + 1));

                header->Nibble[index] = (ushort)(Header.Size + header->MemoryUsed);

                // enough memory to write
                header->MemoryUsed += neededMemory;

                return true;
            }

            return false;
        }

        public static bool TryGet(in Page page, NibblePath key, out ReadOnlySpan<byte> value,
            IInternalTransaction manager)
        {
            var slice = key.SliceFrom(KeySlice);
            var index = GetKeyIndex(key);

            // encode key only once and then compare encoded
            Span<byte> rawKey = stackalloc byte[slice.HexEncodedLength];
            slice.WriteTo(rawKey);

            return TryGetImpl(page, index, in rawKey, out value, manager);
        }

        private static bool TryGetImpl(in Page page, int index, in Span<byte> rawKey, out ReadOnlySpan<byte> value,
            IInternalTransaction manager)
        {
            var header = (Header*)page._ptr;
            var addr = header->Nibble[index];

            if (addr != NullAddr)
            {
                var pagePayload = new Span<byte>(page._ptr, PageSize);
                while (addr != NullAddr)
                {
                    var search = pagePayload.Slice(addr);
                    var leftover = search.Slice(rawKey.Length);

                    if (search.StartsWith(rawKey))
                    {
                        var valueLength = leftover[AddressSize];
                        value = leftover.Slice(AddressSize + LengthOfLenght, valueLength);
                        return true;
                    }

                    addr = ReadUnaligned<ushort>(ref AsRef(in leftover[0]));
                }
            }

            // always check overflow if exists, this page might have been saturated before the current nibble was set 
            if (header->OverflowTo != Null)
            {
                var overflow = manager.GetAt(header->OverflowTo);
                return TryGetImpl(overflow, index, rawKey, out value, manager);
            }

            value = default;
            return false;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static int GetKeyIndex(in NibblePath key) => (key.GetAt(0) << 2) | (key.GetAt(1) >> 2);
    }
}