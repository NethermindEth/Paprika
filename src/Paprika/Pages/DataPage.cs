using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Paprika.Crypto;

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

    [DebuggerStepThrough]
    public DataPage(Page page) => _page = page;

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
        [FieldOffset(sizeof(int))] private fixed int BucketsData[BucketCount];

        /// <summary>
        /// Map of <see cref="BucketsData"/>.
        /// </summary>
        [FieldOffset(sizeof(int))] private DbAddress Bucket;

        public Span<DbAddress> Buckets => MemoryMarshal.CreateSpan(ref Bucket, BucketCount);

        /// <summary>
        /// Data for storing frames.
        /// </summary>
        [FieldOffset(FramesDataOffset)] private fixed byte FramesData[FrameCount * AccountFrame.Size];

        /// <summary>
        /// Map of <see cref="FramesData"/> as a type to allow ref to it.
        /// </summary>
        [FieldOffset(FramesDataOffset)] private AccountFrame Frame;

        /// <summary>
        /// Access all the frames.
        /// </summary>
        public Span<AccountFrame> Frames => MemoryMarshal.CreateSpan(ref Frame, FrameCount);
    }

    /// <summary>
    /// Sets values for the given <see cref="SetContext.Key"/>
    /// </summary>
    /// <param name="ctx"></param>
    /// <param name="batch"></param>
    /// <param name="level">The nesting level of the call</param>
    /// <returns>
    /// The actual page which handled the set operation. Due to page being COWed, it may be a different page.
    /// 
    /// </returns>
    public Page Set(in SetContext ctx, IBatchContext batch, byte level)
    {
        if (Header.PageHeader.BatchId != batch.BatchId)
        {
            // the page is from another batch, meaning, it's readonly. Copy
            var writable = batch.GetWritableCopy(_page);
            new DataPage(writable).Set(ctx, batch, level);
            return writable;
        }

        var frames = Data.Frames;
        var path = NibblePath.FromKey(ctx.Key.BytesAsSpan, level);
        var bucketId = Data.Buckets[path.FirstNibble];

        // try update existing
        while (bucketId.TryGetSamePage(out var frameIndex))
        {
            ref var frame = ref frames[frameIndex];

            if (frame.Key.Equals(ctx.Key))
            {
                // update
                frame.Balance = ctx.Balance;
                frame.Nonce = ctx.Nonce;

                return _page;
            }

            // jump to the next
            bucketId = frame.Next;
        }

        // fail to update, insert
        ref var bucket = ref Data.Buckets[path.FirstNibble];
        if (BitExtensions.TrySetLowestBit(ref Data.FrameUsed, Payload16.FrameCount, out var reserved))
        {
            ref var frame = ref Data.Frames[reserved];

            frame.Key = ctx.Key;
            frame.Balance = ctx.Balance;
            frame.Nonce = ctx.Nonce;

            // set the next to create the linked list
            frame.Next = bucket;

            // overwrite the bucket with the recent one
            bucket = DbAddress.JumpToFrame(reserved, bucket);
            return _page;
        }
        
        // failed to find an empty frame,
        // select a bucket to empty and proceed with creating a child page
        
        

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

    public void GetAccount(in Keccak key, out Account result, byte level)
    {
        var path = NibblePath.FromKey(key.BytesAsSpan, level);

        var frames = Data.Frames;
        var bucket = Data.Buckets[path.FirstNibble];
        
        while (bucket.TryGetSamePage(out var frameIndex))
        {
            ref var frame = ref frames[frameIndex];

            if (frame.Key.Equals(key))
            {
                result = new Account(frame.Balance, frame.Nonce);
                return;
            }

            bucket = frame.Next;
        }

        result = default;
    }
}