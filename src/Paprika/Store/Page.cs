using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Paprika.Data;

namespace Paprika.Store;

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

public interface IPageWithData<TPage> : IPage
    where TPage : struct, IPageWithData<TPage>
{
    /// <summary>
    /// Wraps the raw page as <typeparamref name="TPage"/>
    /// </summary>
    static abstract TPage Wrap(Page page);

    bool TryGet(scoped NibblePath key, IReadOnlyBatchContext batch, out ReadOnlySpan<byte> result);

    Page Set(in NibblePath key, in ReadOnlySpan<byte> data, IBatchContext batch);

    void Report(IReporter reporter, IPageResolver resolver, int level);

    void Accept(IPageVisitor visitor, IPageResolver resolver, DbAddress addr);
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
    public const byte CurrentVersion = 1;

    public const int Size = sizeof(ulong);

    /// <summary>
    /// The id of the last batch that wrote to this page.
    /// </summary>
    [FieldOffset(0)] public uint BatchId;

    /// <summary>
    /// The version of the Paprika the page was written by.
    /// </summary>
    [FieldOffset(4)] public byte PaprikaVersion;

    /// <summary>
    /// The type of the page.
    /// </summary>
    [FieldOffset(5)] public PageType PageType;

    /// <summary>
    /// The depth of the tree.
    /// </summary>
    [FieldOffset(6)] public byte Level;

    /// <summary>
    /// Internal metadata of the given page.
    /// </summary>
    [FieldOffset(7)] public byte Metadata;
}

/// <summary>
/// Struct representing data oriented page types.
/// Two separate types are used: Value and Jump page.
/// Jump pages consist only of jumps according to a part of <see cref="NibblePath"/>.
/// Value pages have buckets + skip list for storing values.
/// </summary>
public readonly unsafe struct Page : IPage, IEquatable<Page>
{
    public const int PageSize = 4 * 1024;

    private readonly byte* _ptr;

    public Page(byte* ptr) => _ptr = ptr;

    public UIntPtr Raw => new(_ptr);

    public byte* Payload => _ptr + PageHeader.Size;

    public void Clear() => Span.Clear();

    public Span<byte> Span => new(_ptr, PageSize);

    public ref PageHeader Header => ref Unsafe.AsRef<PageHeader>(_ptr);

    public bool Equals(Page other) => _ptr == other._ptr;

    public override bool Equals(object? obj) => obj is Page other && Equals(other);

    public override int GetHashCode() => unchecked((int)(long)_ptr);

    public static Page DevOnlyNativeAlloc() =>
        new((byte*)NativeMemory.AlignedAlloc((UIntPtr)PageSize, (UIntPtr)PageSize));
}