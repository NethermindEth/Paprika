using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Paprika.Db;

namespace Paprika.Data;

/// <summary>
/// A markup interface for pages.
/// </summary>
/// <remarks>
/// The page needs to:
/// 1. have one field only that is of type <see cref="Page"/>.
/// 2. have a header that starts with
/// </remarks>
public interface IPage
{
}

/// <summary>
/// Convenient methods for transforming page types.
/// </summary>
public static class PageExtensions
{
    public static void CopyTo<TPage>(this TPage page, TPage destination) where TPage : unmanaged, IPage =>
        Unsafe.As<TPage, Page>(ref page).Span.CopyTo(Unsafe.As<TPage, Page>(ref destination).Span);

    public static void CopyTo<TPage>(this TPage page, Page destination) where TPage : unmanaged, IPage =>
        Unsafe.As<TPage, Page>(ref page).Span.CopyTo(destination.Span);

    public static Page AsPage<TPage>(this TPage page) where TPage : unmanaged, IPage =>
        Unsafe.As<TPage, Page>(ref page);

    public static TPage Cast<TPage>(this Page page) where TPage : unmanaged, IPage =>
        Unsafe.As<Page, TPage>(ref page);
}

/// <summary>
/// The header shared across all the pages.
/// </summary>
[StructLayout(LayoutKind.Explicit, Pack = 1, Size = Size)]
public struct PageHeader
{
    public const int Size = sizeof(ulong);

    /// <summary>
    /// The id of the last batch that wrote to this page.
    /// </summary>
    [FieldOffset(0)] public uint BatchId;

    [FieldOffset(4)] public uint Reserved; // for not it's just alignment
}

/// <summary>
/// Struct representing data oriented page types.
/// Two separate types are used: Value and Jump page.
/// Jump pages consist only of jumps according to a part of <see cref="NibblePath"/>.
/// Value pages have buckets + skip list for storing values.
/// </summary>
public readonly unsafe struct Page : IPage
{
    public const int PageCount = 0x0100_0000; // 64GB addressable
    public const int PageAddressMask = PageCount - 1;
    public const int PageSize = 4 * 1024;

    private readonly byte* _ptr;

    public Page(byte* ptr) => _ptr = ptr;

    public UIntPtr Raw => new(_ptr);

    public byte* Payload => _ptr + PageHeader.Size;

    public void Clear() => Span.Clear();

    public Span<byte> Span => new(_ptr, PageSize);

    public ref PageHeader Header => ref Unsafe.AsRef<PageHeader>(_ptr);
}