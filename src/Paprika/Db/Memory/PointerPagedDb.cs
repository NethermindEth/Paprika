using System.Diagnostics;
using System.Runtime.CompilerServices;
using Paprika.Data;

namespace Paprika.Db.Memory;

public abstract unsafe class PointerPagedDb : PagedDb
{
    protected PointerPagedDb(ulong size, byte historyDepth, Action<IBatchMetrics>? reporter)
        : base(size, historyDepth, reporter)
    {
    }

    protected abstract void* Ptr { get; }

    public override Page GetAt(DbAddress address)
    {
        Debug.Assert(address.IsValidPageAddress, "The address page is invalid and breaches max page count");

        // Long here is required! Otherwise int overflow will turn it to negative value!
        // ReSharper disable once SuggestVarOrType_BuiltInTypes
        long offset = ((long)(int)address) * Page.PageSize;
        return new Page((byte*)Ptr + offset);
    }

    protected override DbAddress GetAddress(in Page page)
    {
        return DbAddress.Page((uint)(Unsafe
            .ByteOffset(ref Unsafe.AsRef<byte>(Ptr), ref Unsafe.AsRef<byte>(page.Raw.ToPointer()))
            .ToInt64() / Page.PageSize));
    }
}