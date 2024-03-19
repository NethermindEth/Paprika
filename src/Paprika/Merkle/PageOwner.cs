using System.Runtime.CompilerServices;
using Paprika.Chain;
using Paprika.Store;

namespace Paprika.Merkle;

/// <summary>
/// Represents an owner over a <see cref="Page"/> with an implementation of a stack over a ref to <see cref="UIntPtr"/>.
/// To manage stack, use <see cref="Rent"/> and <see cref="ReturnStack"/>.
/// </summary>
public readonly ref struct PageOwner
{
    private readonly Page _page;
    private readonly ref UIntPtr _stack;

    private PageOwner(Page page, ref UIntPtr stack)
    {
        _page = page;
        _stack = ref stack;
    }

    public Span<byte> Span => _page.Span;

    public unsafe void Dispose()
    {
        // First, write the root on the page.
        Unsafe.WriteUnaligned(_page.Payload, _stack);

        // Then update the root to the page.
        _stack = _page.Raw;
    }

    public static unsafe PageOwner Rent(BufferPool pool, ref UIntPtr top)
    {
        // Get the current root
        var current = top;

        // Nothing currently memoized, rent new
        if (current == UIntPtr.Zero)
        {
            return new PageOwner(pool.Rent(false), ref top);
        }

        // reuse memoized
        var page = new Page((byte*)current.ToPointer());
        top = Unsafe.ReadUnaligned<UIntPtr>(page.Payload);

        return new PageOwner(page, ref top);
    }

    public static unsafe void ReturnStack(BufferPool pool, ref UIntPtr root)
    {
        while (root != UIntPtr.Zero)
        {
            var page = new Page((byte*)root.ToPointer());
            root = Unsafe.ReadUnaligned<UIntPtr>(page.Payload);
            pool.Return(page);
        }
    }
}