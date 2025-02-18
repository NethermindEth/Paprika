using System.Runtime.InteropServices;
using Paprika.Store;

namespace Paprika.Benchmarks;

public static class Allocator
{
    public static unsafe void* AllocAlignedPage(bool clean = true)
    {
        const UIntPtr size = Page.PageSize;
        var memory = NativeMemory.AlignedAlloc(size, size);
        if (clean)
        {
            NativeMemory.Clear(memory, size);
        }

        return memory;
    }
}