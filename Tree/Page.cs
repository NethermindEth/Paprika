using System.Buffers.Binary;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using static System.Runtime.CompilerServices.Unsafe;

namespace Tree;

public readonly unsafe struct Page
{
    public const int PageCount = 0x0100_0000; // 64GB addressable

    public const int PageSize = 4 * 1024;

    private const int MaxFanOutLevel = 1;

    private readonly void* _ptr;

    public Page(void* ptr)
    {
        _ptr = ptr;
    }

    public UIntPtr Raw => new(_ptr);

    public void Clear() => new Span<byte>(_ptr, PageSize).Clear();

    public void CopyTo(in Page page) => new Span<byte>(_ptr, PageSize).CopyTo(new Span<byte>(page._ptr, PageSize));

    public Page Set(in NibblePath key, in ReadOnlySpan<byte> value, int depth, IPageManager manager)
    {
        var page = this;

        if (!manager.IsWritable(page))
        {
            // if page is not writable, copy it first
            page = manager.GetClean(out _);
            CopyTo(page);
            manager.Abandon(this);
        }

        PageType type = page.Type;
        
        if (type == PageType.None)
        {
            // type not set yet, define it on the basis of the fan out level
            page.Type = type = depth > MaxFanOutLevel ? PageType.ValuePage : PageType.JumpPage;
        }

        if (type == PageType.JumpPage)
        {
            return JumpPage.Set(page, key, value, depth, manager);
        }

        if (type == PageType.ValuePage)
        {
            return ValuePage.Set(page, key, value, manager);
        }

        return page;
    }
    
    private PageType Type
    {
        get => Head & PageType.Mask;
        set { Head = value; }
    }

    private ref PageType Head => ref AsRef<PageType>(_ptr);

    /// <summary>
    /// Represents the page type.
    /// Values of this do not overlap with AddressMask, allowing
    /// </summary>
    [Flags]
    enum PageType : byte
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

        Mask = JumpPage | ValuePage,
    }

    static class JumpPage
    {
        private const int Null = 0;
        private const int AddressSize = 4;

        /// <summary>
        /// Tries to find the <paramref name="path"/> in the given page.
        /// </summary>
        public static bool TryFind(in Page page, ref NibblePath path, int depth, out int pageAddress)
        {
            pageAddress = ReadAddr(page, path, depth);
            path = SlicePath(path, depth);

            return pageAddress != Null;
        }

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
            var addr = (actual << 8) | (int)PageType.JumpPage;
            if (!BitConverter.IsLittleEndian)
            {
                addr = BinaryPrimitives.ReverseEndianness(addr);    
            }
            
            Write(addrPtr, addr);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static int OneOnOdd(int depth) => depth & 1;

        public static Page Set(in Page page, in NibblePath key, in ReadOnlySpan<byte> value, int depth,
            IPageManager manager)
        {
            var addr = ReadAddr(page, key, depth);

            if (addr == Null)
            {
                // does not exist, get and set
                manager.GetClean(out addr);
            }

            var child = manager.GetAt(addr);

            // remember, the child might have changed to another ref
            var actual = manager.GetAddress(child.Set(SlicePath(key, depth), value, depth + 1, manager));

            WriteAddr(page, key, depth, actual);

            return page;
        }
    }

    static class ValuePage
    {
        [StructLayout(LayoutKind.Explicit, Size = Size)]
        private struct Header
        {
            public const int NibbleStart = 8;
            public const int Size = NibbleStart + NibbleCount * AddressSize;

            [FieldOffset(2)] public ushort MemoryUsed;
            [FieldOffset(4)] public int OverflowTo;
            [FieldOffset(NibbleStart)] public fixed ushort Nibble[NibbleCount];
        }

        private const int NibbleCount = 16;
        private const int AddressSize = 2;
        private const int AvailableMemoryInPage = PageSize - Header.Size;
        private const int LengthOfLenght = 1;
        private const int Null = 0;

        public static Page Set(in Page page, in NibblePath key, in ReadOnlySpan<byte> value, IPageManager manager)
        {
            if (TryWrite(page, key, value))
            {
                return page;
            }

            // slow path, overflow
            ref var header = ref AsRef<Header>(page._ptr);
            if (header.OverflowTo != Null)
            {
                var overflow = manager.GetAt(header.OverflowTo);
                if (!manager.IsWritable(overflow))
                {
                    var copy = manager.GetClean(out header.OverflowTo);
                    overflow.CopyTo(copy);
                    manager.Abandon(overflow);
                    overflow = copy;
                }

                Set(in overflow, key, value, manager);
            }

            var clean = manager.GetClean(out header.OverflowTo);
            Set(in clean, key, value, manager);
            return page;
        }

        private static bool TryWrite(Page page, NibblePath key, ReadOnlySpan<byte> value)
        {
            ref var header = ref AsRef<Header>(page._ptr);
            var nibble = key.FirstNibble;
            ref var addr = ref header.Nibble[nibble];

            var neededMemory = (ushort)(key.RawByteLength + AddressSize + LengthOfLenght + value.Length);

            var available = AvailableMemoryInPage - header.MemoryUsed;
            if (available >= neededMemory)
            {
                var destination = new Span<byte>(Add<byte>(page._ptr, Header.Size + header.MemoryUsed), neededMemory);

                var leftover = key.WriteTo(destination);
                WriteUnaligned(ref leftover[0], addr);
                leftover[AddressSize] = (byte)value.Length;
                value.CopyTo(leftover.Slice(AddressSize + 1));

                addr = (ushort)(Header.Size + header.MemoryUsed);

                // enough memory to write
                header.MemoryUsed += neededMemory;

                return true;
            }

            return false;
        }
    }
}

public interface IPageManager
{
    /// <summary>
    /// Gets information whether the page is writable, meaning,
    /// that it was returned as unused 
    /// </summary>
    bool IsWritable(in Page page);

    /// <summary>
    /// Gets the page at given address.
    /// </summary>
    /// <param name="address"></param>
    /// <returns></returns>
    Page GetAt(int address);

    int GetAddress(in Page page);

    /// <summary>
    /// Gets an unused page
    /// </summary>
    /// <returns></returns>
    Page GetClean(out int addr);

    /// <summary>
    /// Abandons the page, marking it as reusable once the transaction commits.
    /// </summary>
    /// <param name="page"></param>
    void Abandon(in Page page);
}

public unsafe class MemoryPageManager : IPageManager, IDisposable
{
    private readonly void* _writable;
    private readonly int _bitmapSize;
    
    private readonly HashSet<int> _abandoned = new();
    private readonly Stack<int> _reusable = new();

    private readonly void* _ptr;
    private readonly int _maxPage;

    private int _nextPage;

    public MemoryPageManager(ulong size)
    {
        _ptr = NativeMemory.AlignedAlloc((UIntPtr)size, (UIntPtr)Page.PageSize);
        _maxPage = (int)(size / Page.PageSize);

        _bitmapSize = _maxPage / 8 + 1;
        _writable = NativeMemory.Alloc((UIntPtr)_bitmapSize);
    }

    public double TotalUsedPages => (double)(_nextPage - _abandoned.Count) / _maxPage;

    public bool IsWritable(in Page page)
    {
        var address = GetAddress(page);
        var @byte = AsRef<byte>(Add<byte>(_writable, address / 8));
        return (@byte & (1 << (address & 7))) != 0;
    }

    public Page GetAt(int address)
    {
        if (address > _maxPage)
            throw new ArgumentException($"Requested address {address} while the max page is {_maxPage}");
        
        return new Page(Add<byte>(_ptr, address * Page.PageSize));
    }

    public int GetAddress(in Page page)
    {
        return (int)(ByteOffset(ref AsRef<byte>(_ptr), ref AsRef<byte>(page.Raw.ToPointer()))
            .ToInt64() / Page.PageSize);
    }

    public Page GetClean(out int addr)
    {
        if (!_reusable.TryPop(out addr))
        {
            if (_nextPage >= _maxPage)
                throw new OutOfMemoryException("Not enough memory with page manager");

            addr = _nextPage;
            
            // set bit
            ref var @byte = ref AsRef<byte>(Add<byte>(_writable, addr / 8));
            @byte = (byte)(@byte | (1 << (addr & 7)));
            
            _nextPage += 1;
        }

        var page = GetAt(addr);
        page.Clear();
        return page;
    }

    public void Abandon(in Page page) => _abandoned.Add(GetAddress(page));

    public void Commit()
    {
        // simulate the commit with the clear
        new Span<byte>(_writable, _bitmapSize).Clear();

        foreach (var page in _abandoned)
        {
            _reusable.Push(page);
        }

        _abandoned.Clear();
    }

    public void Dispose() => NativeMemory.AlignedFree(_ptr);
}