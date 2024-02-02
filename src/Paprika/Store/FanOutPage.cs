using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Paprika.Data;

namespace Paprika.Store;

[method: DebuggerStepThrough]
public readonly unsafe struct FanOutPage(Page page) : IPageWithData<FanOutPage>
{
    public static FanOutPage Wrap(Page page) => new(page);

    private const int ConsumedNibbles = 2;

    public ref PageHeader Header => ref page.Header;

    private ref Payload Data => ref Unsafe.AsRef<Payload>(page.Payload);

    [StructLayout(LayoutKind.Explicit, Size = Size)]
    private struct Payload
    {
        private const int Size = FanOut * DbAddress.Size;

        /// <summary>
        /// The number of buckets to fan out to.
        /// </summary>
        private const int FanOut = 256;

        /// <summary>
        /// The first item of map of frames to allow ref to it.
        /// </summary>
        [FieldOffset(0)] private DbAddress Address;

        public Span<DbAddress> Addresses => MemoryMarshal.CreateSpan(ref Address, FanOut);
    }

    public bool TryGet(scoped NibblePath key, IPageResolver batch, out ReadOnlySpan<byte> result)
    {
        if (key.Length < ConsumedNibbles)
        {
            result = default;
            return false;
        }

        var index = GetIndex(key);

        var addr = Data.Addresses[index];
        if (addr.IsNull)
        {
            result = default;
            return false;
        }

        return new DataPage(batch.GetAt(addr)).TryGet(key.SliceFrom(ConsumedNibbles), batch, out result);
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

        var index = GetIndex(key);
        var sliced = key.SliceFrom(ConsumedNibbles);

        ref var addr = ref Data.Addresses[index];

        if (addr.IsNull)
        {
            var newPage = batch.GetNewPage(out addr, true);
            newPage.Header.PageType = PageType.Standard;
            newPage.Header.Level = 2;

            new DataPage(newPage).Set(sliced, data, batch);
            return page;
        }

        // update after set
        addr = batch.GetAddress(new DataPage(batch.GetAt(addr)).Set(sliced, data, batch));
        return page;
    }

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
}