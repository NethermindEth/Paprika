using System.Runtime.InteropServices;
using Paprika.Data;

namespace Paprika.Db.Memory;

public unsafe class NativeMemoryPagedDb : PointerPagedDb
{
    private readonly void* _ptr;

    public NativeMemoryPagedDb(ulong size, byte historyDepth, Action<IBatchMetrics>? reporter = null) : base(size, historyDepth, reporter)
    {
        _ptr = NativeMemory.AlignedAlloc((UIntPtr)size, (UIntPtr)Page.PageSize);
        NativeMemory.Clear(_ptr, (UIntPtr)size);

        RootInit();
    }

    protected override void* Ptr => _ptr;
    public override void Dispose() => NativeMemory.AlignedFree(_ptr);

    protected override void FlushAllPages()
    {
        // no op
    }

    protected override void FlushRootPage(in Page rootPage)
    {
        // no op
    }
}