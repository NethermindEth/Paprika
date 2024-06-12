using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using FluentAssertions;
using Paprika.Store;

namespace Paprika.Tests.Store;

/// <summary>
/// Tests ensuring that unmanaged structs do no leak out beyond their sizes.
/// </summary>
public class MemoryFencingTests
{
    [Test]
    public void Int_for_sanity()
    {
        Alloc<int>(sizeof(int)).Should().Be(0);
    }

    [Test]
    public void AbandonedPage()
    {
        ref var list = ref Alloc<AbandonedList>(AbandonedList.Size);
        list.IsFullyEmpty.Should().BeTrue();
    }

    [TearDown]
    public unsafe void TearDown()
    {
        while (_alignedAllocations.TryPop(out var alloc))
        {
            NativeMemory.AlignedFree(alloc.ToPointer());
        }
    }

    private readonly Stack<UIntPtr> _alignedAllocations = new();

    private unsafe ref T Alloc<T>(int size, int alignment = sizeof(int))
        where T : struct
    {
        const int fenceSize = 32;

        var sizeTotal = (UIntPtr)size + fenceSize * 2;
        var memory = NativeMemory.AlignedAlloc(sizeTotal, (UIntPtr)alignment);
        NativeMemory.Clear(memory, sizeTotal);

        _alignedAllocations.Push((UIntPtr)memory);

        var actualStart = new UIntPtr(memory) + fenceSize;

        Fence(memory);
        Fence((actualStart + (uint)size).ToPointer());

        return ref Unsafe.AsRef<T>(actualStart.ToPointer());

        static void Fence(void* ptr)
        {
            var span = new Span<byte>(ptr, fenceSize);
            const byte fenceFilling = 0xFF;
            span.Fill(fenceFilling);
        }
    }
}