using System.Runtime.InteropServices;
using Paprika.Pages;

namespace Paprika.Db;

public unsafe class NativeMemoryPagedDb : PagedDb
{
    private readonly void* _ptr;

    public NativeMemoryPagedDb(ulong size, byte historyDepth) : base(size, historyDepth)
    {
        _ptr = NativeMemory.AlignedAlloc((UIntPtr)size, (UIntPtr)Page.PageSize);
        NativeMemory.Clear(_ptr, (UIntPtr)size);

        RootInit();
    }

    protected override void* Ptr => _ptr;
    public override void Dispose() => NativeMemory.AlignedFree(_ptr);

    protected override void Flush()
    {
        // no op
    }
}