using System.Runtime.InteropServices;
using Paprika.Data;

namespace Paprika.Db.Memory;

public unsafe class NativeMemoryPageManager : PointerPageManager
{
    private readonly void* _ptr;

    public NativeMemoryPageManager(ulong size) : base(size)
    {
        _ptr = NativeMemory.AlignedAlloc((UIntPtr)size, (UIntPtr)Page.PageSize);
        NativeMemory.Clear(_ptr, (UIntPtr)size);
    }

    protected override void* Ptr => _ptr;

    public override void Dispose() => NativeMemory.AlignedFree(_ptr);

    public override void FlushAllPages()
    {
        // no op
    }

    public override void FlushRootPage(in Page rootPage)
    {
        // no op
    }
}