using System.Runtime.CompilerServices;
using System.Runtime.Intrinsics;
using Paprika.Store;

namespace Paprika.Data;

/// <summary>
/// Provides extensions for <see cref="Page"/> that are data related.
/// </summary>
public static class PageExtensions
{
    [SkipLocalsInit]
    [MethodImpl(MethodImplOptions.AggressiveInlining | MethodImplOptions.AggressiveOptimization)]
    public static unsafe void OrWith(this Page @this, Page other)
    {
        const int bitsPerByte = 8;
        const int unroll = 2;

        ref var a = ref Unsafe.AsRef<byte>(@this.Raw.ToPointer());
        ref var b = ref Unsafe.AsRef<byte>(other.Raw.ToPointer());

        if (Vector512.IsHardwareAccelerated)
        {
            const int size = 512 / bitsPerByte;

            for (UIntPtr i = 0; i < Page.PageSize; i += size * unroll)
            {
                var va1 = Vector512.LoadUnsafe(ref a, i);
                var vb1 = Vector512.LoadUnsafe(ref b, i);
                var vc1 = Vector512.BitwiseOr(va1, vb1);

                vc1.StoreUnsafe(ref a, i);

                var va2 = Vector512.LoadUnsafe(ref a, i + size);
                var vb2 = Vector512.LoadUnsafe(ref b, i + size);
                var vc2 = Vector512.BitwiseOr(va2, vb2);

                vc2.StoreUnsafe(ref a, i + size);
            }
        }
        else if (Vector256.IsHardwareAccelerated)
        {
            const int size = 256 / bitsPerByte;

            for (UIntPtr i = 0; i < Page.PageSize; i += size * unroll)
            {
                var va1 = Vector256.LoadUnsafe(ref a, i);
                var vb1 = Vector256.LoadUnsafe(ref b, i);
                var vc1 = Vector256.BitwiseOr(va1, vb1);

                vc1.StoreUnsafe(ref a, i);

                var va2 = Vector256.LoadUnsafe(ref a, i + size);
                var vb2 = Vector256.LoadUnsafe(ref b, i + size);
                var vc2 = Vector256.BitwiseOr(va2, vb2);

                vc2.StoreUnsafe(ref a, i + size);
            }
        }
        else if (Vector128.IsHardwareAccelerated)
        {
            const int size = 128 / bitsPerByte;

            for (UIntPtr i = 0; i < Page.PageSize; i += size * unroll)
            {
                var va1 = Vector128.LoadUnsafe(ref a, i);
                var vb1 = Vector128.LoadUnsafe(ref b, i);
                var vc1 = Vector128.BitwiseOr(va1, vb1);

                vc1.StoreUnsafe(ref a, i);

                var va2 = Vector128.LoadUnsafe(ref a, i + size);
                var vb2 = Vector128.LoadUnsafe(ref b, i + size);
                var vc2 = Vector128.BitwiseOr(va2, vb2);

                vc2.StoreUnsafe(ref a, i + size);
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