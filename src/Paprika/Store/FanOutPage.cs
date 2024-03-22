using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Paprika.Data;

namespace Paprika.Store;

/// <summary>
/// The fan out page stores keys shorter than <see cref="ConsumedNibbles"/> in its content,
/// and delegates other keys lengths to lower layers of the tree.
///
/// This is useful to get a good fan out at the higher levels of the tree.
/// Unfortunately this impacts the caching behavior and may result in more pages being updated.
/// </summary>
[method: DebuggerStepThrough]
public readonly unsafe struct FanOutPage(Page page) : IPageWithData<FanOutPage>
{
    public static FanOutPage Wrap(Page page) => new(page);

    private const int ConsumedNibbles = 2;

    private ref PageHeader Header => ref page.Header;

    private ref Payload Data => ref Unsafe.AsRef<Payload>(page.Payload);

    [StructLayout(LayoutKind.Explicit, Size = Size)]
    private struct Payload
    {
        private const int Size = Page.PageSize - PageHeader.Size;

        private const int FanOutSize = FanOut * DbAddress.Size;

        private const int DataSize = Size - FanOutSize;

        /// <summary>
        /// The number of buckets to fan out to.
        /// </summary>
        private const int FanOut = 256;

        /// <summary>
        /// The first item of map of frames to allow ref to it.
        /// </summary>
        [FieldOffset(0)] private DbAddress Address;

        public Span<DbAddress> Addresses => MemoryMarshal.CreateSpan(ref Address, FanOut);

        [FieldOffset(FanOutSize)] private byte DataFirst;

        public Span<byte> Data => MemoryMarshal.CreateSpan(ref DataFirst, DataSize);
    }

    public bool TryGet(IReadOnlyBatchContext batch, scoped in NibblePath key, out ReadOnlySpan<byte> result)
    {
        batch.AssertRead(Header);

        if (IsKeyLocal(key))
        {
            return new SlottedArray(Data.Data).TryGet(key, out result);
        }

        var index = GetIndex(key);

        var addr = Data.Addresses[index];
        if (addr.IsNull)
        {
            result = default;
            return false;
        }

        return new DataPage(batch.GetAt(addr)).TryGet(batch, key.SliceFrom(ConsumedNibbles), out result);
    }

    private static int GetIndex(scoped in NibblePath key) => (key.GetAt(0) << NibblePath.NibbleShift) + key.GetAt(1);

    public Page Set(in NibblePath key, in ReadOnlySpan<byte> data, IBatchContext batch)
    {
        if (Header.BatchId != batch.BatchId)
        {
            // the page is from another batch, meaning, it's readonly. Copy
            var writable = batch.GetWritableCopy(page);
            return new FanOutPage(writable).Set(key, data, batch);
        }

        if (IsKeyLocal(key))
        {
            if (new SlottedArray(Data.Data).TrySet(key, data) == false)
            {
                ThrowNoSpaceInline();
            }

            return page;
        }

        var index = GetIndex(key);
        var sliced = key.SliceFrom(ConsumedNibbles);

        ref var addr = ref Data.Addresses[index];

        if (addr.IsNull)
        {
            var newPage = batch.GetNewPage(out addr, true);
            newPage.Header.PageType = Header.PageType;
            newPage.Header.Level = 2;

            new DataPage(newPage).Set(sliced, data, batch);
            return page;
        }

        // update after set
        addr = batch.GetAddress(new DataPage(batch.GetAt(addr)).Set(sliced, data, batch));
        return page;
    }

    private static bool IsKeyLocal(in NibblePath key) => key.Length < ConsumedNibbles;

    [DoesNotReturn]
    [StackTraceHidden]
    private static void ThrowNoSpaceInline() => throw new Exception("Could not set the data inline");

    public void Report(IReporter reporter, IPageResolver resolver, int level)
    {
        foreach (var bucket in Data.Addresses)
        {
            if (!bucket.IsNull)
            {
                new DataPage(resolver.GetAt(bucket)).Report(reporter, resolver, level + 1);
            }
        }
    }

    public void Accept(IPageVisitor visitor, IPageResolver resolver, DbAddress addr)
    {
        using var scope = visitor.On(this, addr);

        foreach (var bucket in Data.Addresses)
        {
            if (!bucket.IsNull)
            {
                new DataPage(resolver.GetAt(bucket)).Accept(visitor, resolver, bucket);
            }
        }
    }
}