﻿using System.Runtime.InteropServices;
using System.Text;
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

        Assert.True(new DataPage(updated).TryGet(Key0, out var context, RootLevel));
        Assert.AreEqual(Nonce0, context.Nonce);
        Assert.AreEqual(Balance0, context.Balance);
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

        Assert.True(new DataPage(updated).TryGet(Key0, out var context, RootLevel));
        Assert.AreEqual(Nonce1, context.Nonce);
        Assert.AreEqual(Balance1, context.Balance);
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

        Assert.True(new DataPage(updated).TryGet(Key1a, out var result, RootLevel));
        Assert.AreEqual(Nonce0, result.Nonce);
        Assert.AreEqual(Balance0, result.Balance);

        Assert.True(new DataPage(updated).TryGet(Key1b, out result, RootLevel));
        Assert.AreEqual(Nonce1, result.Nonce);
        Assert.AreEqual(Balance1, result.Balance);
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

        page = new Page((byte*)NativeMemory.AlignedAlloc((UIntPtr)Page.PageSize, (UIntPtr)sizeof(long)));
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

        public Page GetNewDirtyPage(out DbAddress addr)
        {
            var page = AllocPage();
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

            var @new = GetNewDirtyPage(out _);
            page.CopyTo(@new);
            @new.Header.BatchId = BatchId;
            return @new;
        }
    }
}