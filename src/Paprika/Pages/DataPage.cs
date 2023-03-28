using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace Paprika.Pages;

public readonly unsafe struct DataPage : IPage
{
    private const int AddressSize = 4;
    
    private readonly Page _page;

    public DataPage(byte* ptr) : this(new Page(ptr))
    {
    }

    public DataPage(Page root) => _page = root;

    public ref DataPageHeader Header => ref Unsafe.As<PageHeader, DataPageHeader>(ref _page.Header);
    
    /// <summary>
    /// The payload of the data page that stores 16 nibble-addressable buckets.
    /// These buckets is used to store up to <see cref="FrameCount"/> entries before flushing them down as other pages
    /// like page split. 
    /// </summary>
    [StructLayout(LayoutKind.Explicit, Size = Size)]
    public struct Payload16
    {
        public const int Size = Page.PageSize - DataPageHeader.Size;
        
        public const int BucketCount = 16;
        
        private const int FramesDataOffset = sizeof(int) + sizeof(int) + BucketCount * AddressSize;
        private const int FrameCount = (Size - FramesDataOffset) / AccountFrame.Size;

        /// <summary>
        /// The bit map of frames used at this page.
        /// </summary>
        [FieldOffset(0)] public int FrameUsed;

        /// <summary>
        /// The nibble addressable buckets.
        /// </summary>
        [FieldOffset(sizeof(int))] public fixed int Buckets[BucketCount];

        /// <summary>
        /// Data for storing frames.
        /// </summary>
        [FieldOffset(FramesDataOffset)]
        public fixed byte FramesData[FrameCount * AccountFrame.Size];
        
        [FieldOffset(FramesDataOffset)]
        public AccountFrame Frame;

        public Span<AccountFrame> Frames => MemoryMarshal.CreateSpan(ref Frame, FrameCount);
    }
    
    [StructLayout(LayoutKind.Explicit, Size = Size)]
    public struct DataPageHeader
    {
        public const int Size = PageHeader.Size + sizeof(DataPageFlags);

        [FieldOffset(0)]
        public PageHeader PageHeader;

        [FieldOffset(PageHeader.Size)]
        public DataPageFlags Flags;
    }

    /// <summary>
    /// All the flags for the <see cref="DataPage"/>s.
    /// </summary>
    public enum DataPageFlags : int
    {
        // type of the page, and others
    }
}