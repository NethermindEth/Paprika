using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Paprika.Crypto;

namespace Paprika.Pages;

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

public interface IDataPage : IPage
{
    bool TryGet(FixedMap.Key key, IReadOnlyBatchContext batch, out ReadOnlySpan<byte> result);

    Page Set(in SetContext ctx);
}

public static class DataPageExtensions
{
    public static Account GetAccount<TPage>(this TPage page, NibblePath path, IReadOnlyBatchContext ctx)
        where TPage : IDataPage
    {
        if (page.TryGet(FixedMap.Key.Account(path), ctx, out var result))
        {
            Serializer.Account.ReadAccount(result, out var balance, out var nonce);
            return new Account(balance, nonce);
        }

        return default;
    }

    public static Page SetAccount<TPage>(this TPage page, NibblePath key, in Account account, IBatchContext batch)
        where TPage : IDataPage
    {
        Span<byte> payload = stackalloc byte[Serializer.Account.EOAMaxByteCount];
        payload = Serializer.Account.WriteEOATo(payload, account.Balance, account.Nonce);
        var ctx = new SetContext(FixedMap.Key.Account(key), payload, batch);
        return page.Set(ctx);
    }
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