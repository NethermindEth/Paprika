using System.Runtime.InteropServices;
using System.Text;
using Nethermind.Int256;
using NUnit.Framework;
using Paprika.Crypto;
using Paprika.Pages;

namespace Paprika.Tests;

public unsafe class DataPageTests
{
    private static readonly Keccak Key0 = Keccak.Compute(Encoding.UTF8.GetBytes(nameof(Key0)));
    private static readonly Keccak Key1 = Keccak.Compute(Encoding.UTF8.GetBytes(nameof(Key1)));
    private static readonly Keccak Key2 = Keccak.Compute(Encoding.UTF8.GetBytes(nameof(Key2)));

    private static readonly UInt256 Balance0 = 13;
    private static readonly UInt256 Balance1 = 17;
    private static readonly UInt256 Balance3 = 19;

    private static readonly UInt256 Nonce0 = 23;
    private static readonly UInt256 Nonce1 = 29;
    private static readonly UInt256 Nonce2 = 31;

    private static readonly byte RootLevel = 0;

    [Test]
    public void Test()
    {
        const uint batchId = 1;

        var page = AllocPage();
        page.Clear();

        var batch = new BatchContext { BatchId = batchId };
        var dataPage = new DataPage(page);
        var ctx = new SetContext(Key0, Balance0, Nonce0);

        var updated = dataPage.Set(ctx, batch, RootLevel);

        Assert.True(new DataPage(updated).TryGetNonce(Key0, out var nonce, RootLevel));
        Assert.AreEqual(Nonce0, nonce);
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