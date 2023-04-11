using System.Runtime.CompilerServices;

namespace Paprika.Pages;

public static class PageExtensions
{
    public static void Clear<TPage>(this TPage page) where TPage : unmanaged, IPage =>
        Unsafe.As<TPage, Page>(ref page).Clear();

    public static UIntPtr Raw<TPage>(this TPage page) where TPage : unmanaged, IPage =>
        Unsafe.As<TPage, Page>(ref page).Raw;

    public static Span<byte> AsSpan<TPage>(this TPage page) where TPage : unmanaged, IPage =>
        Unsafe.As<TPage, Page>(ref page).Span;

    public static void CopyTo<TPage>(this TPage page, TPage destination) where TPage : unmanaged, IPage =>
        Unsafe.As<TPage, Page>(ref page).Span.CopyTo(Unsafe.As<TPage, Page>(ref destination).Span);

    public static void CopyTo<TPage>(this TPage page, Page destination) where TPage : unmanaged, IPage =>
        Unsafe.As<TPage, Page>(ref page).Span.CopyTo(destination.Span);

    public static Page AsPage<TPage>(this TPage page) where TPage : unmanaged, IPage =>
        Unsafe.As<TPage, Page>(ref page);
}