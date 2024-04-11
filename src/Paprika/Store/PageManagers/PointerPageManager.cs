using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;

namespace Paprika.Store.PageManagers;

public abstract unsafe class PointerPageManager(long size) : IPageManager
{
    public int MaxPage { get; } = (int)(size / Page.PageSize);

    protected abstract void* Ptr { get; }

    public Page GetAt(DbAddress address)
    {
        if (address.Raw >= MaxPage)
        {
            ThrowInvalidPage(address.Raw);
        }

        return new Page((byte*)Ptr + address.FileOffset);

        [DoesNotReturn]
        [StackTraceHidden]
        void ThrowInvalidPage(uint raw)
        {
            throw new IndexOutOfRangeException($"The database breached its size! Requested page {raw} from max {MaxPage}. The returned page is invalid");
        }
    }

    public DbAddress GetAddress(in Page page)
    {
        return DbAddress.Page((uint)(Unsafe
            .ByteOffset(ref Unsafe.AsRef<byte>(Ptr), ref Unsafe.AsRef<byte>(page.Raw.ToPointer()))
            .ToInt64() / Page.PageSize));
    }

    public virtual Page GetAtForWriting(DbAddress address, bool reused) => GetAt(address);

    public abstract ValueTask FlushPages(ICollection<DbAddress> addresses, CommitOptions options);

    public abstract ValueTask FlushRootPage(DbAddress rootPage, CommitOptions options);

    public abstract void Flush();

    public abstract void ForceFlush();

    public abstract void Dispose();
}
