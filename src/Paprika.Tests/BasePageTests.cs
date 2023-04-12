using System.Runtime.InteropServices;
using NUnit.Framework;
using Paprika.Pages;

namespace Paprika.Tests;

public abstract class BasePageTests
{
    private static readonly Stack<Page> Pages = new();
    private static readonly List<Page> All = new();

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

    protected static unsafe Page AllocPage()
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

    internal class TestBatchContext : BatchContextBase
    {
        private readonly Dictionary<DbAddress, Page> _address2Page = new();

        // data pages should start at non-null addresses
        // 0-N is take by metadata pages
        private uint _pageCount = 1U;

        public TestBatchContext(uint batchId) : base(batchId) { }


        public override Page GetAt(DbAddress address) => _address2Page[address];

        public override Page GetNewPage(out DbAddress addr, bool clear)
        {
            var page = AllocPage();
            if (clear)
                page.Clear();

            page.Header.BatchId = BatchId;

            addr = DbAddress.Page(_pageCount++);

            _address2Page[addr] = page;

            return page;
        }

        protected override void RegisterForFutureReuse(Page page)
        {
            // NOOP
        }

        public override string ToString() => $"Batch context used {_pageCount} pages to write the data";
    }

    internal static TestBatchContext NewBatch(uint batchId) => new(batchId);
}