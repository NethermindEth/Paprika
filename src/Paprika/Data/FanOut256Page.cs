using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Paprika.Db;

namespace Paprika.Pages;

/// <summary>
/// Represents the page with the fan out of 256 to maximally flatten the tree.
/// </summary>
public readonly unsafe struct FanOut256Page : IDataPage
{
    private readonly Page _page;

    [DebuggerStepThrough]
    public FanOut256Page(Page page) => _page = page;

    public ref PageHeader Header => ref _page.Header;
    public ref Payload Data => ref Unsafe.AsRef<Payload>(_page.Payload);

    /// <summary>
    /// Represents the data of this data page. This type of payload stores data in 256 nibble-addressable buckets.
    /// No data are stored inline.
    /// </summary>
    [StructLayout(LayoutKind.Explicit, Size = Size)]
    public struct Payload
    {
        private const int Size = Page.PageSize - PageHeader.Size;

        public const int BucketCount = 256;

        /// <summary>
        /// The first field of buckets.
        /// </summary>
        [FieldOffset(0)] private DbAddress Bucket;

        public Span<DbAddress> Buckets => MemoryMarshal.CreateSpan(ref Bucket, BucketCount);
    }

    public Page Set(in SetContext ctx)
    {
        if (Header.BatchId != ctx.Batch.BatchId)
        {
            // the page is from another batch, meaning, it's readonly. Copy
            var writable = ctx.Batch.GetWritableCopy(_page);
            return new FanOut256Page(writable).Set(ctx);
        }

        var prefix = FirstTwoNibbles(ctx.Key.Path);

        var address = Data.Buckets[prefix];

        if (address.IsNull)
        {
            // no data page, allocate and set
            var page = ctx.Batch.GetNewPage(out address, true);
            new DataPage(page).Set(ctx.SliceFrom(NibbleCount));
            Data.Buckets[prefix] = address;
        }
        else
        {
            var page = ctx.Batch.GetAt(address);
            var updated = new DataPage(page).Set(ctx.SliceFrom(NibbleCount));
            Data.Buckets[prefix] = ctx.Batch.GetAddress(updated);
        }

        return _page;
    }

    public static ushort FirstTwoNibbles(NibblePath path)
    {
        return (ushort)((path.GetAt(0) << NibblePath.NibbleShift * 0) +
                        (path.GetAt(1) << NibblePath.NibbleShift * 1));
    }

    private const int NibbleCount = 2;

    public bool TryGet(FixedMap.Key key, IReadOnlyBatchContext batch, out ReadOnlySpan<byte> result)
    {
        var prefix = FirstTwoNibbles(key.Path);

        var address = Data.Buckets[prefix];

        if (address.IsNull)
        {
            result = default;
            return false;
        }

        return new DataPage(batch.GetAt(address)).TryGet(key.SliceFrom(NibbleCount), batch, out result);
    }
}