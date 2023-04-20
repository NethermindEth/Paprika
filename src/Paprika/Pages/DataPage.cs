using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Paprika.Crypto;
using Paprika.Pages.Frames;

namespace Paprika.Pages;

/// <summary>
/// Represents a data page storing account data.
/// </summary>
/// <remarks>
/// The page is capable of storing some data inside of it and provides fan out for lower layers.
/// This means that for small amount of data no creation of further layers is required.
///
/// The page preserves locality of the data though. It's either all the children with a given nibble stored
/// in the parent page, or they are flushed underneath. 
/// </remarks>
public readonly unsafe struct DataPage : IPage
{
    private readonly Page _page;

    [DebuggerStepThrough]
    public DataPage(Page page) => _page = page;

    public ref PageHeader Header => ref _page.Header;

    public ref Payload Data => ref Unsafe.AsRef<Payload>(_page.Payload);

    /// <summary>
    /// Represents the data of this data page. This type of payload stores data in 16 nibble-addressable buckets.
    /// These buckets is used to store up to <see cref="FrameCount"/> entries before flushing them down as other pages
    /// like page split. 
    /// </summary>
    [StructLayout(LayoutKind.Explicit, Size = Size)]
    public struct Payload
    {
        private const int Size = Page.PageSize - PageHeader.Size;

        public const int BucketCount = 16;

        /// <summary>
        /// The offset to the first frame.
        /// </summary>
        private const int FramesDataOffset = sizeof(int) + BitPool64.Size + BucketCount * DbAddress.Size;

        /// <summary>
        /// How many frames fit in this page.
        /// </summary>
        public const int FrameCount = (Size - FramesDataOffset) / EOAFrame.Size;

        /// <summary>
        /// The bit map of frames used at this page.
        /// </summary>
        [FieldOffset(0)] public BitPool64 FrameUsed;

        /// <summary>
        /// The nibble addressable buckets.
        /// </summary>
        [FieldOffset(BitPool64.Size)] private fixed int BucketsData[BucketCount];

        /// <summary>
        /// Map of <see cref="BucketsData"/>.
        /// </summary>
        [FieldOffset(BitPool64.Size)] private DbAddress Bucket;

        public Span<DbAddress> Buckets => MemoryMarshal.CreateSpan(ref Bucket, BucketCount);

        /// <summary>
        /// The first item of map of frames to allow ref to it.
        /// </summary>
        [FieldOffset(FramesDataOffset)] private EOAFrame Frame;

        /// <summary>
        /// Access all the frames.
        /// </summary>
        public Span<EOAFrame> Frames => MemoryMarshal.CreateSpan(ref Frame, FrameCount);
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
    public Page Set(in SetContext ctx, int level)
    {
        if (Header.BatchId != ctx.Batch.BatchId)
        {
            // the page is from another batch, meaning, it's readonly. Copy
            var writable = ctx.Batch.GetWritableCopy(_page);
            return new DataPage(writable).Set(ctx, level);
        }

        var frames = Data.Frames;
        var nibble = NibblePath.FromKey(ctx.Key.BytesAsSpan, level).FirstNibble;

        var address = Data.Buckets[nibble];

        // the bucket is not null and represents a page jump, follow it
        if (address.IsNull == false && address.IsValidPageAddress)
        {
            var page = ctx.Batch.GetAt(address);
            var updated = new DataPage(page).Set(ctx, level + 1);

            // remember the updated
            Data.Buckets[nibble] = ctx.Batch.GetAddress(updated);
            return _page;
        }

        // try update existing
        if (TryFindFrameInBucket(address, ctx.Key, out var frameIndex))
        {
            ref var frame = ref frames[frameIndex.Value];

            frame.Balance = ctx.Balance;
            frame.Nonce = ctx.Nonce;

            return _page;
        }

        // fail to update, insert if there's place
        if (Data.FrameUsed.TrySetLowestBit(Payload.FrameCount, out var reserved))
        {
            ref var frame = ref frames[reserved];

            frame.Key = ctx.Key;
            frame.Balance = ctx.Balance;
            frame.Nonce = ctx.Nonce;

            // set the next to create the linked list
            address.TryGetFrameIndex(out var previousFrameIndex);
            frame.Header = FrameHeader.BuildEOA(previousFrameIndex);

            // overwrite the bucket with the recent one
            Data.Buckets[nibble] = DbAddress.JumpToFrame(FrameIndex.FromIndex(reserved), Data.Buckets[nibble]);
            return _page;
        }

        // failed to find an empty frame,
        // select a bucket to empty and proceed with creating a child page
        // there must be at least one as otherwise it would be propagated down to the page
        var biggestBucket = DbAddress.Null;
        var index = -1;

        for (var i = 0; i < Payload.BucketCount; i++)
        {
            if (Data.Buckets[i].IsSamePage && Data.Buckets[i].SamePageJumpCount > biggestBucket.SamePageJumpCount)
            {
                biggestBucket = Data.Buckets[i];
                index = i;
            }
        }

        // address is set to the most counted
        var child = ctx.Batch.GetNewPage(out Data.Buckets[index], true);
        var dataPage = new DataPage(child);

        // copy the data pointed by address to the new dataPage, clean up its bits from reserved frames
        biggestBucket.TryGetFrameIndex(out var biggestFrameChain);

        while (biggestFrameChain.IsNull == false)
        {
            ref var frame = ref frames[biggestFrameChain.Value];

            var set = new SetContext(frame.Key, frame.Balance, frame.Nonce, ctx.Batch);
            dataPage.Set(set, (byte)(level + 1));

            // the frame is no longer used, clear it
            Data.FrameUsed.ClearBit(biggestFrameChain.Value);

            // jump to the next
            biggestFrameChain = frame.Header.NextFrame;
        }

        // there's a place on this page now, add it again
        Set(ctx, level);

        return _page;
    }

    public void GetAccount(in Keccak key, IReadOnlyBatchContext batch, out Account result, int level)
    {
        var nibble = NibblePath.FromKey(key.BytesAsSpan, level).FirstNibble;
        var bucket = Data.Buckets[nibble];

        // non-null page jump, follow it!
        if (bucket.IsNull == false && bucket.IsValidPageAddress)
        {
            new DataPage(batch.GetAt(bucket)).GetAccount(key, batch, out result, level + 1);
            return;
        }

        if (TryFindFrameInBucket(bucket, key, out var index))
        {
            ref readonly var frame = ref Data.Frames[index.Value];
            result = new Account(frame.Balance, frame.Nonce);
            return;
        }

        result = default;
    }

    /// <summary>
    /// Tries to find a frame with a matching <paramref name="key"/> within a given <paramref name="bucket"/>.
    /// </summary>
    private bool TryFindFrameInBucket(DbAddress bucket, in Keccak key, out FrameIndex index)
    {
        var frames = Data.Frames;
        if (bucket.TryGetFrameIndex(out var frameIndex))
        {
            while (frameIndex.IsNull == false)
            {
                ref readonly var frame = ref frames[frameIndex.Value];

                if (frame.Key.Equals(key))
                {
                    index = frameIndex;
                    return true;
                }

                frameIndex = frame.Header.NextFrame;
            }
        }

        index = default;
        return false;
    }
}