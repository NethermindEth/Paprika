using System.Runtime.CompilerServices;
using System.Runtime.Intrinsics;
using Paprika.Store;

namespace Paprika.Data;

/// <summary>
/// Provides extensions for <see cref="Page"/> that are data related.
/// </summary>
public static class PageExtensions
{
    [MethodImpl(MethodImplOptions.AggressiveInlining | MethodImplOptions.AggressiveOptimization)]
    public static unsafe void OrWith(this Page @this, Page other)
    {
        const int bitsPerByte = 8;

        ref var a = ref Unsafe.AsRef<byte>(@this.Raw.ToPointer());
        ref var b = ref Unsafe.AsRef<byte>(other.Raw.ToPointer());

        if (Vector512.IsHardwareAccelerated)
        {
            const int size = 512 / bitsPerByte;

            for (UIntPtr i = 0; i < Page.PageSize; i += size)
            {
                var va = Vector512.LoadUnsafe(ref a, i);
                var vb = Vector512.LoadUnsafe(ref b, i);
                var vc = Vector512.BitwiseOr(va, vb);

                vc.StoreUnsafe(ref a, i);
            }
        }
        else if (Vector256.IsHardwareAccelerated)
        {
            const int size = 256 / bitsPerByte;

            for (UIntPtr i = 0; i < Page.PageSize; i += size)
            {
                var va = Vector256.LoadUnsafe(ref a, i);
                var vb = Vector256.LoadUnsafe(ref b, i);
                var vc = Vector256.BitwiseOr(va, vb);

                vc.StoreUnsafe(ref a, i);
            }
        }
        else if (Vector128.IsHardwareAccelerated)
        {
            const int size = 128 / bitsPerByte;

            for (UIntPtr i = 0; i < Page.PageSize; i += size)
            {
                var va = Vector128.LoadUnsafe(ref a, i);
                var vb = Vector128.LoadUnsafe(ref b, i);
                var vc = Vector128.BitwiseOr(va, vb);

                vc.StoreUnsafe(ref a, i);
            }
        }
        else
        {
            const int size = sizeof(long);

            for (var i = 0; i < Page.PageSize / size; i++)
            {
                ref var va = ref Unsafe.As<byte, long>(ref Unsafe.Add(ref a, i * size));
                var vb = Unsafe.As<byte, long>(ref Unsafe.Add(ref b, i * size));

                va |= vb;
            }
        }
    }
}