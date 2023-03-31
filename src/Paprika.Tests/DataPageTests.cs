﻿using System.Buffers.Binary;
using System.Runtime.InteropServices;
using FluentAssertions;
using Nethermind.Int256;
using NUnit.Framework;
using Paprika.Crypto;
using Paprika.Pages;

namespace Paprika.Tests;

public unsafe class DataPageTests
{
    private static readonly Keccak Key0 = new(new byte[]
        { 0, 1, 2, 3, 4, 5, 6, 7, 0, 1, 2, 3, 4, 5, 6, 7, 0, 1, 2, 3, 4, 5, 6, 7, 0, 1, 2, 3, 4, 5, 6, 7, });

    private static readonly Keccak Key1a = new(new byte[]
        { 1, 1, 2, 3, 4, 5, 6, 7, 0, 1, 2, 3, 4, 5, 6, 7, 0, 1, 2, 3, 4, 5, 6, 7, 0, 1, 2, 3, 4, 5, 6, 7, });

    private static readonly Keccak Key1b = new(new byte[]
        { 1, 1, 2, 3, 4, 5, 6, 7, 0, 1, 2, 3, 4, 5, 6, 7, 0, 1, 2, 3, 4, 5, 6, 7, 0, 1, 2, 3, 4, 5, 6, 8 });

    private static readonly UInt256 Balance0 = 13;
    private static readonly UInt256 Balance1 = 17;
    private static readonly UInt256 Balance3 = 19;

    private static readonly UInt256 Nonce0 = 23;
    private static readonly UInt256 Nonce1 = 29;
    private static readonly UInt256 Nonce2 = 31;

    private const byte RootLevel = 0;

    const uint BatchId = 1;

    [Test]
    public void Set_then_Get()
    {
        var page = AllocPage();
        page.Clear();

        var batch = new BatchContext { BatchId = BatchId };
        var dataPage = new DataPage(page);
        var ctx = new SetContext(Key0, Balance0, Nonce0);

        var updated = dataPage.Set(ctx, batch, RootLevel);

        new DataPage(updated).GetAccount(Key0, batch, out var account, RootLevel);

        Assert.AreEqual(Nonce0, account.Nonce);
        Assert.AreEqual(Balance0, account.Balance);
    }

    [Test]
    public void Update_key()
    {
        var page = AllocPage();
        page.Clear();

        var batch = new BatchContext { BatchId = BatchId };

        var dataPage = new DataPage(page);
        var ctx1 = new SetContext(Key0, Balance0, Nonce0);
        var ctx2 = new SetContext(Key0, Balance1, Nonce1);

        var updated = dataPage.Set(ctx1, batch, RootLevel);
        updated = new DataPage(updated).Set(ctx2, batch, RootLevel);

        new DataPage(updated).GetAccount(Key0, batch, out var account, RootLevel);
        Assert.AreEqual(Nonce1, account.Nonce);
        Assert.AreEqual(Balance1, account.Balance);
    }

    [Test]
    public void Works_with_bucket_collision()
    {
        var page = AllocPage();
        page.Clear();

        var batch = new BatchContext { BatchId = BatchId };

        var dataPage = new DataPage(page);
        var ctx1 = new SetContext(Key1a, Balance0, Nonce0);
        var ctx2 = new SetContext(Key1b, Balance1, Nonce1);

        var updated = dataPage.Set(ctx1, batch, RootLevel);
        updated = new DataPage(updated).Set(ctx2, batch, RootLevel);

        new DataPage(updated).GetAccount(Key1a, batch, out var account, RootLevel);
        Assert.AreEqual(Nonce0, account.Nonce);
        Assert.AreEqual(Balance0, account.Balance);

        new DataPage(updated).GetAccount(Key1b, batch, out account, RootLevel);
        Assert.AreEqual(Nonce1, account.Nonce);
        Assert.AreEqual(Balance1, account.Balance);
    }

    [Test]
    public void Page_overflows()
    {
        var page = AllocPage();
        page.Clear();

        var batch = new BatchContext { BatchId = BatchId };
        var dataPage = new DataPage(page);

        const int count = 2 * 1024 * 1024;

        for (uint i = 0; i < count; i++)
        {
            var key = Key1a;
            BinaryPrimitives.WriteUInt32LittleEndian(key.BytesAsSpan, i);

            var ctx = new SetContext(key, i, i);
            dataPage = new DataPage(dataPage.Set(ctx, batch, RootLevel));
        }

        for (uint i = 0; i < count; i++)
        {
            var key = Key1a;
            BinaryPrimitives.WriteUInt32LittleEndian(key.BytesAsSpan, i);

            dataPage.GetAccount(key, batch, out var account, RootLevel);
            account.Should().Be(new Account(i, i));
        }
    }

    [Test]
    public void SetUp()
    {
        Pages.Clear();

        foreach (var page in All)
        {
            Pages.Push(page);
        }

        All.Clear();
    }


    private static readonly Stack<Page> Pages = new();
    private static readonly List<Page> All = new();

    private static Page AllocPage()
    {
        if (Pages.TryPop(out var page))
            return page;

        const int slab = 16;

        var memory = (byte*)NativeMemory.AlignedAlloc((UIntPtr)Page.PageSize * (nuint)slab, (UIntPtr)sizeof(long));

        for (var i = 1; i < slab; i++)
        {
            var item = new Page(memory + Page.PageSize * i);
            Pages.Push(item);
            All.Add(item);
        }

        page = new Page(memory);
        All.Add(page); // memo for reuse
        return page;
    }

    class BatchContext : IBatchContext
    {
        private readonly Dictionary<DbAddress, Page> _address2Page = new();
        private readonly Dictionary<UIntPtr, DbAddress> _page2Address = new();

        private uint _pageCount;

        public Page GetAt(DbAddress address) => _address2Page[address];

        public DbAddress GetAddress(in Page page) => _page2Address[page.Raw];

        public Page GetNewPage(out DbAddress addr, bool clear)
        {
            var page = AllocPage();
            if (clear)
                page.Clear();

            page.Header.BatchId = BatchId;

            addr = DbAddress.Page(_pageCount++);

            _address2Page[addr] = page;
            _page2Address[page.Raw] = addr;

            return page;
        }

        public long BatchId { get; set; }

        public Page GetWritableCopy(Page page)
        {
            if (page.Header.BatchId == BatchId)
                return page;

            var @new = GetNewPage(out _, true);
            page.CopyTo(@new);
            @new.Header.BatchId = BatchId;
            return @new;
        }

        public override string ToString() => $"Batch context used {_pageCount} pages to write the data";
    }
}