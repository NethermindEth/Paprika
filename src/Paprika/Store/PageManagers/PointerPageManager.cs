using System.Diagnostics;
using System.Runtime.CompilerServices;
using Paprika.Data;

namespace Paprika.Store.PageManagers;

public abstract unsafe class PointerPageManager : IPageManager
{
    public int MaxPage { get; }

    protected PointerPageManager(ulong size) => MaxPage = (int)(size / Page.PageSize);

    protected abstract void* Ptr { get; }

    public Page GetAt(DbAddress address)
    {
        if (address.Raw >= MaxPage)
        {
            throw new IndexOutOfRangeException("The database breached its size! The returned page is invalid");
        }

        Debug.Assert(address.IsValidPageAddress, "The address page is invalid and breaches max page count");

        // Long here is required! Otherwise int overflow will turn it to negative value!
        // ReSharper disable once SuggestVarOrType_BuiltInTypes
        long offset = ((long)(int)address) * Page.PageSize;
        return new Page((byte*)Ptr + offset);
    }

    public DbAddress GetAddress(in Page page)
    {
        return DbAddress.Page((uint)(Unsafe
            .ByteOffset(ref Unsafe.AsRef<byte>(Ptr), ref Unsafe.AsRef<byte>(page.Raw.ToPointer()))
            .ToInt64() / Page.PageSize));
    }

    public abstract void FlushAllPages();
    public abstract void FlushRootPage(in Page rootPage);

    public abstract void Dispose();
}