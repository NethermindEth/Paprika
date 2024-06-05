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
        ref var a = ref Unsafe.AsRef<byte>(@this.Raw.ToPointer());
        ref var b = ref Unsafe.AsRef<byte>(other.Raw.ToPointer());

        if (Vector512.IsHardwareAccelerated)
        {
            const int size = 512;
            const int unroll = 2;

            for (var i = 0; i < Page.PageSize / size / unroll; i += unroll)
            {
                var va = Vector512.LoadUnsafe(ref Unsafe.Add(ref a, i * size));
                var vb = Vector512.LoadUnsafe(ref Unsafe.Add(ref b, i * size));

                var vc = Vector512.BitwiseOr(va, vb);
                vc.StoreUnsafe(ref Unsafe.Add(ref a, i * size));

                va = Vector512.LoadUnsafe(ref Unsafe.Add(ref a, (i + 1) * size));
                vb = Vector512.LoadUnsafe(ref Unsafe.Add(ref b, (i + 1) * size));

                vc = Vector512.BitwiseOr(va, vb);
                vc.StoreUnsafe(ref Unsafe.Add(ref a, (i + 1) * size));
            }
        }
        else if (Vector256.IsHardwareAccelerated)
        {
            const int size = 256;
            const int unroll = 2;
            const int count = Page.PageSize / size / unroll;

            for (var i = 0; i < count; i += unroll)
            {
                var va = Vector256.LoadUnsafe(ref Unsafe.Add(ref a, i * size));
                var vb = Vector256.LoadUnsafe(ref Unsafe.Add(ref b, i * size));

                var vc = Vector256.BitwiseOr(va, vb);
                vc.StoreUnsafe(ref Unsafe.Add(ref a, i * size));

                va = Vector256.LoadUnsafe(ref Unsafe.Add(ref a, (i + 1) * size));
                vb = Vector256.LoadUnsafe(ref Unsafe.Add(ref b, (i + 1) * size));

                vc = Vector256.BitwiseOr(va, vb);
                vc.StoreUnsafe(ref Unsafe.Add(ref a, (i + 1) * size));
            }
        }
        else if (Vector128.IsHardwareAccelerated)
        {
            const int size = 128;
            const int unroll = 2;
            const int count = Page.PageSize / size / unroll;

            for (var i = 0; i < count; i += unroll)
            {
                var va = Vector128.LoadUnsafe(ref Unsafe.Add(ref a, i * size));
                var vb = Vector128.LoadUnsafe(ref Unsafe.Add(ref b, i * size));

                var vc = Vector128.BitwiseOr(va, vb);
                vc.StoreUnsafe(ref Unsafe.Add(ref a, i * size));

                va = Vector128.LoadUnsafe(ref Unsafe.Add(ref a, (i + 1) * size));
                vb = Vector128.LoadUnsafe(ref Unsafe.Add(ref b, (i + 1) * size));

                vc = Vector128.BitwiseOr(va, vb);
                vc.StoreUnsafe(ref Unsafe.Add(ref a, (i + 1) * size));
            }
        }
        else if (Vector64.IsHardwareAccelerated)
        {
            const int size = 64;
            for (var i = 0; i < Page.PageSize / size; i++)
            {
                var va = Vector64.LoadUnsafe(ref Unsafe.Add(ref a, i * size));
                var vb = Vector64.LoadUnsafe(ref Unsafe.Add(ref b, i * size));

                var vc = Vector64.BitwiseOr(va, vb);
                vc.StoreUnsafe(ref Unsafe.Add(ref a, i * size));
            }
        }
        else
        {
            const int size = 8;
            for (var i = 0; i < Page.PageSize / size; i++)
            {
                ref var va = ref Unsafe.As<byte, long>(ref Unsafe.Add(ref a, i * size));
                var vb = Unsafe.As<byte, long>(ref Unsafe.Add(ref b, i * size));

                va |= vb;
            }
        }
    }
}