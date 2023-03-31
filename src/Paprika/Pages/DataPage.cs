using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Paprika.Crypto;
using Paprika.Db;

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
    public Page Set(in SetContext ctx, IBatchContext batch, int level)
    {
        if (Header.PageHeader.BatchId != batch.BatchId)
        {
            // the page is from another batch, meaning, it's readonly. Copy
            var writable = batch.GetWritableCopy(_page);
            return new DataPage(writable).Set(ctx, batch, level);
        }

        var frames = Data.Frames;
        var nibble = NibblePath.FromKey(ctx.Key.BytesAsSpan, level).FirstNibble;

        var address = Data.Buckets[nibble];

        // the bucket represents a page jump, follow it
        if (address.IsNull == false && address.IsValidAddressPage)
        {
            var page = batch.GetAt(address);
            new DataPage(page).Set(ctx, batch, level + 1);
            return _page;
        }

        // try update existing
        while (address.TryGetSamePage(out var frameIndex))
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
            address = frame.Next;
        }

        // fail to update, insert
        ref var bucket = ref Data.Buckets[nibble];
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
        // there must be at least one as otherwise it would be propagated down to the page
        var biggestBucket = DbAddress.Null;
        var index = -1;

        for (var i = 0; i < Payload16.BucketCount; i++)
        {
            if (Data.Buckets[i].IsSamePage && Data.Buckets[i].SamePageJumpCount > biggestBucket.SamePageJumpCount)
            {
                biggestBucket = Data.Buckets[i];
                index = i;
            }
        }

        // address is set to the most counted
        var child = batch.GetNewPage(out Data.Buckets[index], true);
        var dataPage = new DataPage(child);

        // copy the data pointed by address to the new dataPage, clean up its bits from reserved frames
        while (biggestBucket.TryGetSamePage(out var frameIndex))
        {
            ref var frame = ref frames[frameIndex];

            var set = new SetContext(frame.Key, frame.Balance, frame.Nonce);
            dataPage.Set(set, batch, (byte)(level + 1));

            // the frame is no longer used, clear it
            BitExtensions.ClearBit(ref Data.FrameUsed, frameIndex);

            // jump to the next
            biggestBucket = frame.Next;
        }

        // there's a place on this page now, add it again
        Set(ctx, batch, level);

        return _page;
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

    public void GetAccount(in Keccak key, IBatchContext batch, out Account result, int level)
    {
        var frames = Data.Frames;
        var nibble = NibblePath.FromKey(key.BytesAsSpan, level).FirstNibble;
        var bucket = Data.Buckets[nibble];

        if (bucket.IsNull == false && bucket.IsValidAddressPage)
        {
            new DataPage(batch.GetAt(bucket)).GetAccount(key, batch, out result, level + 1);
            return;
        }

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