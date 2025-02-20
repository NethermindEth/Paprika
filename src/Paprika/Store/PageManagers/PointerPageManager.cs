using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;
using System.Runtime.Intrinsics.X86;

namespace Paprika.Store.PageManagers;

public abstract unsafe class PointerPageManager(long size) : IPageManager
{
    public int MaxPage { get; } = (int)(size / Page.PageSize);

    protected abstract void* Ptr { get; }

    public void Prefetch(DbAddress addr)
    {
        if (Sse.IsSupported)
        {
            if (addr.IsNull || addr.Raw > (uint)MaxPage)
            {
                return;
            }

            // Fetch to L2 cache as we don't know if will need it
            // So don't pollute L1 cache
            Sse.Prefetch1((byte*)Ptr + addr.FileOffset);
        }
    }

    public Page GetAt(DbAddress address)
    {
        if (address.Raw >= MaxPage)
        {
            ThrowInvalidPage(address.Raw);
        }

        return new Page((byte*)Ptr + address.FileOffset);
    }

    public bool IsValidAddress(DbAddress address) => address.Raw < MaxPage;

    [DoesNotReturn]
    [StackTraceHidden]
    private void ThrowInvalidPage(uint raw) => throw new IndexOutOfRangeException(
        $"The database breached its size! Requested page {raw} from max {MaxPage}. The returned page is invalid");

    public DbAddress GetAddress(in Page page)
    {
        var addr = DbAddress.Page((uint)(Unsafe
            .ByteOffset(ref Unsafe.AsRef<byte>(Ptr), ref Unsafe.AsRef<byte>(page.Raw.ToPointer()))
            .ToInt64() / Page.PageSize));

        Debug.Assert(IsValidAddress(addr));

        return addr;
    }

    public abstract ValueTask WritePages(ICollection<DbAddress> addresses, CommitOptions options);

    public ValueTask WritePages(IEnumerable<(DbAddress at, Page page)> pages, CommitOptions options)
    {
        var copyTime = Stopwatch.StartNew();

        // The memory was copied to a set of pages that are not mapped. Requires copying back to the mapped ones.
        Parallel.ForEach(pages, (pair, _) =>
        {
            var (at, page) = pair;
            page.CopyTo(GetAt(at));
        });

        ReportCopyTime(copyTime.Elapsed);

        // The memory is now coherent and memory mapped contains what pages passed have.
        var addresses = Interlocked.Exchange(ref _addresses, null) ?? new();
        Debug.Assert(addresses.Count == 0);

        try
        {
            foreach (var (at, _) in pages)
            {
                addresses.Add(at);
            }

            return WritePages(addresses, options);
        }
        finally
        {
            // return
            addresses.Clear();
            Interlocked.CompareExchange(ref _addresses, addresses, null);
        }
    }

    protected abstract void ReportCopyTime(TimeSpan elapsed);

    private List<DbAddress>? _addresses = new();

    public abstract ValueTask WriteRootPage(DbAddress rootPage, CommitOptions options);

    public abstract void Flush();

    public abstract void ForceFlush();

    public abstract void Dispose();
}
