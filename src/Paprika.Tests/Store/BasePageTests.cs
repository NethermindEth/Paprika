using System.Runtime.InteropServices;
using Paprika.Crypto;
using Paprika.Store;

namespace Paprika.Tests.Store;

public abstract class BasePageTests
{
    protected static unsafe Page AllocPage()
    {
        var memory = (byte*)NativeMemory.AlignedAlloc((UIntPtr)Page.PageSize, (UIntPtr)sizeof(long));
        new Span<byte>(memory, Page.PageSize).Clear();
        return new Page(memory);
    }

    internal class TestBatchContext : BatchContextBase
    {
        private readonly Dictionary<DbAddress, Page> _address2Page = new();
        private readonly Dictionary<UIntPtr, DbAddress> _page2Address = new();

        // data pages should start at non-null addresses
        // 0-N is take by metadata pages
        private uint _pageCount = 1U;

        public TestBatchContext(uint batchId) : base(batchId)
        {
            IdCache = new Dictionary<Keccak, uint>();
        }

        public override Page GetAt(DbAddress address) => _address2Page[address];

        public override DbAddress GetAddress(Page page) => _page2Address[page.Raw];

        public override Page GetNewPage(out DbAddress addr, bool clear)
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

        // for now
        public override bool WasWritten(DbAddress addr) => true;
        public override void RegisterForFutureReuse(Page page)
        {
            // NOOP
        }

        public override Dictionary<Keccak, uint> IdCache { get; }

        public override string ToString() => $"Batch context used {_pageCount} pages to write the data";

        public TestBatchContext Next()
        {
            var next = new TestBatchContext(BatchId + 1);

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
    }

    internal static TestBatchContext NewBatch(uint batchId) => new(batchId);
}