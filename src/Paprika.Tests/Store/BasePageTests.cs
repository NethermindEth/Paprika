using System.Runtime.InteropServices;
using FluentAssertions;
using Paprika.Crypto;
using Paprika.Store;

namespace Paprika.Tests.Store;

public abstract class BasePageTests
{
    protected static unsafe Page AllocPage()
    {
        var memory = (byte*)NativeMemory.AlignedAlloc(Page.PageSize, sizeof(long));
        new Span<byte>(memory, Page.PageSize).Clear();
        return new Page(memory);
    }

    internal class TestBatchContext(uint batchId, Stack<DbAddress>? reusable = null) : BatchContextBase(batchId)
    {
        private readonly Dictionary<DbAddress, Page> _address2Page = new();
        private readonly Dictionary<UIntPtr, DbAddress> _page2Address = new();
        private readonly Stack<DbAddress> _reusable = reusable ?? new Stack<DbAddress>();
        private readonly HashSet<DbAddress> _toReuse = new();

        // data pages should start at non-null addresses
        // 0-N is take by metadata pages
        private uint _pageCount = 1U;

        public override Page GetAt(DbAddress address) => _address2Page[address];

        public override DbAddress GetAddress(Page page) => _page2Address[page.Raw];

        public override Page GetNewPage(out DbAddress addr, bool clear)
        {
            Page page;
            if (_reusable.TryPop(out addr))
            {
                page = GetAt(addr);
            }
            else
            {
                page = AllocPage();
                addr = DbAddress.Page(_pageCount++);

                _address2Page[addr] = page;
                _page2Address[page.Raw] = addr;
            }

            if (clear)
                page.Clear();

            page.Header.BatchId = BatchId;

            return page;
        }

        // for now
        public override bool WasWritten(DbAddress addr) => true;

        public override void RegisterForFutureReuse(Page page)
        {
            _toReuse.Add(GetAddress(page))
                .Should()
                .BeTrue("Page should not be registered as reusable before");
        }

        public Page[] FindOlderThan(uint batchId)
        {
            var older = _address2Page
                .Where(kvp => kvp.Value.Header.BatchId < batchId)
                .Select(kvp => kvp.Value)
                .ToArray();

            Array.Sort(older, (a, b) => a.Header.BatchId.CompareTo(b.Header.BatchId));
            return older;
        }

        public override Dictionary<Keccak, uint> IdCache { get; } = new();

        public override string ToString() => $"Batch context used {_pageCount} pages to write the data";

        public TestBatchContext Next()
        {
            var set = new HashSet<DbAddress>();

            // push these to reuse
            foreach (var addr in _toReuse)
            {
                set.Add(addr).Should().BeTrue();
            }

            // push reusable leftovers from this 
            foreach (var addr in _reusable)
            {
                set.Add(addr).Should().BeTrue();
            }

            var next = new TestBatchContext(BatchId + 1, new Stack<DbAddress>(set));

            // remember the mapping
            foreach (var (addr, page) in _address2Page)
            {
                next._address2Page[addr] = page;
            }

            foreach (var (page, addr) in _page2Address)
            {
                next._page2Address[page] = addr;
            }

            // copy the page count so that it properly allocates new ones
            next._pageCount = _pageCount;

            return next;
        }

        public uint PageCount => _pageCount;
    }

    internal static TestBatchContext NewBatch(uint batchId) => new(batchId);
}
