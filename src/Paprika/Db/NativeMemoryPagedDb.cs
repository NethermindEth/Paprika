using System.Runtime.InteropServices;

namespace Paprika.Db;

public unsafe class NativeMemoryPagedDb : PagedDb
{
    private readonly void* _ptr;

    public NativeMemoryPagedDb(ulong size) : base(size)
    {
        _ptr = NativeMemory.AlignedAlloc((UIntPtr)size, (UIntPtr)Page.PageSize);

        RootInit();
    }

    protected override void* Ptr => _ptr;
    public override void Dispose() => NativeMemory.AlignedFree(_ptr);

    protected override void Flush()
    {
        // no op
    }
}