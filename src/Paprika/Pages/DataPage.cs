using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Nethermind.Int256;

namespace Paprika.Pages;

/// <summary>
/// Represents a data page storing account data.
/// </summary>
/// <remarks>
/// The page is capable of storing some data inside of it and provides fan out for lower layers.
/// This means that for small amount of data no creation of further layers is required.
///
/// TODO: the split algo should be implemented properly. 
/// </remarks>
public readonly unsafe struct DataPage : IPage
{
    private readonly Page _page;

    public DataPage(Page root) => _page = root;

    public ref DataPageHeader Header => ref Unsafe.As<PageHeader, DataPageHeader>(ref _page.Header);
    
    public ref Payload16 Data => ref Unsafe.AsRef<Payload16>(_page.Payload);

    /// <summary>
    /// Represents the data of this data page. This type of payload stores data in 16 nibble-addressable buckets.
    /// These buckets is used to store up to <see cref="FrameCount"/> entries before flushing them down as other pages
    /// like page split. 
    /// </summary>
    [StructLayout(LayoutKind.Explicit, Size = Size)]
    public struct Payload16
    {
        public const int Size = Page.PageSize - DataPageHeader.Size;

        public const int BucketCount = 16;

        private const int FramesDataOffset = sizeof(int) + sizeof(int) + BucketCount * DbAddress.Size;
        public const int FrameCount = (Size - FramesDataOffset) / AccountFrame.Size;

        /// <summary>
        /// The bit map of frames used at this page.
        /// </summary>
        [FieldOffset(0)] public uint FrameUsed;

        /// <summary>
        /// The nibble addressable buckets.
        /// </summary>
        [FieldOffset(sizeof(int))] public fixed int BucketsData[BucketCount];

        /// <summary>
        /// Map of <see cref="BucketsData"/>.
        /// </summary>
        [FieldOffset(sizeof(int))]
        public DbAddress Buckets;
        
        /// <summary>
        /// Data for storing frames.
        /// </summary>
        [FieldOffset(FramesDataOffset)]
        public fixed byte FramesData[FrameCount * AccountFrame.Size];

        /// <summary>
        /// Map of <see cref="FramesData"/> as a type to allow ref to it.
        /// </summary>
        [FieldOffset(FramesDataOffset)]
        public AccountFrame Frame;

        /// <summary>
        /// Access all the frames.
        /// </summary>
        public Span<AccountFrame> Frames => MemoryMarshal.CreateSpan(ref Frame, FrameCount);
    }

    public void Set(in SetContext ctx, IInternalTransaction tx, byte level)
    {
        // TODO: updates, check for the key existence, comparisons and more, for now just reserve a slot and set it
        
        if (BitExtensions.TryReserveBit(ref Data.FrameUsed, Payload16.FrameCount, out var reserved))
        {
            var path = NibblePath.FromKey(ctx.Key.BytesAsSpan, level);
            ref var bucket = ref Unsafe.Add(ref Data.Buckets, path.FirstNibble);
            
            ref var frame = ref Data.Frames[reserved];

            frame.Key = ctx.Key;
            frame.Balance = ctx.Balance;
            frame.Nonce = ctx.Nonce;

            // set the next to create the linked list
            frame.Next = bucket;
            
            // overwrite the bucket with the recent one
            bucket = DbAddress.JumpToFrame(reserved);
            return;
        }

        throw new NotImplementedException("Should overflow to the next page or result in a page split");
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

    public bool TryGetNonce(in Keccak key, out UInt256 nonce, byte level)
    {
        var path = NibblePath.FromKey(key.BytesAsSpan, level);
        
        // TODO: updates, check for the key existence, comparisons and more, and nested levels

        var frames = Data.Frames;
        var bucket = Unsafe.Add(ref Data.Buckets, path.FirstNibble);
        while (bucket.IsNull == false)
        {
            ref var frame = ref frames[bucket];

            if (frame.Key.Equals(key))
            {
                nonce = frame.Nonce;
                return true;
            }

            bucket = frame.Next;
        }

        nonce = default;
        return false;
    }
}