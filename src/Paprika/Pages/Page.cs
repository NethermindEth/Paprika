﻿using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Nethermind.Int256;
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

/// <summary>
/// An interface for a page holding data, capable to <see cref="TryGet"/> and <see cref="Set"/> values.
/// </summary>
public interface IDataPage : IPage
{
    bool TryGet(FixedMap.Key key, IReadOnlyBatchContext batch, out ReadOnlySpan<byte> result);

    Page Set(in SetContext ctx);
}

/// <summary>
/// Extension methods used as a poor man derivation across structs implementing <see cref="IDataPage"/>.
/// </summary>
public static class DataPageExtensions
{
    public static Account GetAccount<TPage>(this TPage page, NibblePath path, IReadOnlyBatchContext ctx)
        where TPage : struct, IDataPage
    {
        var key = FixedMap.Key.Account(path);

        if (page.TryGet(key, ctx, out var result))
        {
            Serializer.ReadAccount(result, out var balance, out var nonce);
            return new Account(balance, nonce);
        }

        return default;
    }

    public static Page SetAccount<TPage>(this TPage page, NibblePath path, in Account account, IBatchContext batch)
        where TPage : struct, IDataPage
    {
        var key = FixedMap.Key.Account(path);

        Span<byte> payload = stackalloc byte[Serializer.BalanceNonceMaxByteCount];
        payload = Serializer.WriteAccount(payload, account.Balance, account.Nonce);
        var ctx = new SetContext(key, payload, batch);
        return page.Set(ctx);
    }

    public static UInt256 GetStorage<TPage>(this TPage page, NibblePath path, in Keccak address,
        IReadOnlyBatchContext ctx)
        where TPage : struct, IDataPage
    {
        var key = FixedMap.Key.StorageCell(path, address);

        if (page.TryGet(key, ctx, out var result))
        {
            Serializer.ReadStorageValue(result, out var value);
            return value;
        }

        return default;
    }

    public static Page SetStorage<TPage>(this TPage page, NibblePath path, in Keccak address, in UInt256 value,
        IBatchContext batch)
        where TPage : struct, IDataPage
    {
        var key = FixedMap.Key.StorageCell(path, address);

        Span<byte> payload = stackalloc byte[Serializer.StorageValueMaxByteCount];
        payload = Serializer.WriteStorageValue(payload, value);

        var ctx = new SetContext(key, payload, batch);
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