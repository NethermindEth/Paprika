using System.Diagnostics;
using System.Runtime.InteropServices;
using FluentAssertions;
using NUnit.Framework;
using Paprika.Crypto;
using Paprika.Store;

namespace Paprika.Tests.Store;

public class AbandonedTests : BasePageTests
{
    [Test]
    public void Page_capacity()
    {
        const int expected = 2000;

        var addresses = Enumerable.Range(1, expected).Select(i => new DbAddress((uint)i)).ToArray();

        var page = new AbandonedPage(Page.DevOnlyNativeAlloc());
        page.AsPage().Clear();

        var added = new Stack<DbAddress>();

        foreach (var address in addresses)
        {
            if (page.TryPush(address))
            {
                added.Push(address);
            }
        }

        added.Count.Should().Be(expected);

        while (added.TryPop(out var dequeued))
        {
            page.TryPop(out var popped).Should().BeTrue();
            popped.Should().Be(dequeued);
        }
    }

    private const int HistoryDepth = 2;

    [TestCase(18, 1, 10_000, false, TestName = "Accounts - 1")]
    [TestCase(641, 100, 10_000, false, TestName = "Accounts - 100")]
    [TestCase(26875, 4000, 200, false,
        TestName = "Accounts - 4000 to get a bit reuse",
        Category = Categories.LongRunning)]
    [TestCase(70765, 10_000, 50, false,
        TestName = "Accounts - 10000 to breach the AbandonedPage",
        Category = Categories.LongRunning)]
    [TestCase(98577, 20_000, 50, true,
        TestName = "Storage - 20_000 accounts with a single storage slot",
        Category = Categories.LongRunning)]
    public async Task Reuse_in_limited_environment(int pageCount, int accounts, int repeats, bool isStorage)
    {
        var keccaks = Initialize(accounts);

        // set big value
        var accountValue = new byte[3000];
        new Random(17).NextBytes(accountValue);

        using var db = PagedDb.NativeMemoryDb(pageCount * Page.PageSize, HistoryDepth);

        for (var i = 0; i < repeats; i++)
        {
            using var block = db.BeginNextBatch();
            foreach (var keccak in keccaks)
            {
                if (isStorage)
                {
                    block.SetStorage(keccak, keccak, accountValue);
                }
                else
                {
                    block.SetAccount(keccak, accountValue);
                }
            }

            await block.Commit(CommitOptions.FlushDataAndRoot);
        }

        db.NextFreePage.Should().Be((uint)pageCount,
            "Ensure that the page count is minimal. " +
            "After running the test they should mach the allocated space.");

        var oldPages = new List<uint>();

        for (uint at = 0; at < db.NextFreePage; at++)
        {
            var page = db.GetAt(DbAddress.Page(at));
            if (page.Header.BatchId < (uint)(repeats - HistoryDepth - 1))
            {
                oldPages.Add(at);
            }
        }

        if (oldPages.Count > 0)
        {
            var counters = new int[byte.MaxValue];

            var ages = new Dictionary<uint, uint>();

            foreach (var addr in oldPages)
            {
                var page = db.GetAt(DbAddress.Page(addr));

                if (page.Header.PageType == PageType.Abandoned)
                {
                    ages[addr] = page.Header.BatchId;
                }

                counters[(int)page.Header.PageType]++;
            }

            Console.WriteLine("Abandoned addr->batch: ");
            foreach (var age in ages)
            {
                Console.WriteLine($"  @{age.Key}: {age.Value}");
            }

            foreach (var type in Enum.GetValues<PageType>())
            {
                Console.WriteLine($"{type}: {counters[(int)type]}");
            }
        }

        return;

        static Keccak[] Initialize(int accounts)
        {
            var keccaks = new Keccak[accounts];
            const int seed = 13;
            new Random(seed).NextBytes(MemoryMarshal.Cast<Keccak, byte>(keccaks.AsSpan()));
            return keccaks;
        }
    }

    [Test]
    public async Task Work_proper_bookkeeping_when_lots_of_reads()
    {
        const int repeats = 1_000;
        const int multiplier = 2 + 1; // fanout page + data page + abandoned page per commit
        const int historyDepth = 2;

        var account = Keccak.EmptyTreeHash;

        byte[] value = [13];

        using var db = PagedDb.NativeMemoryDb((multiplier * repeats + historyDepth) * Page.PageSize);

        var reads = new List<IReadOnlyBatch>();

        for (var i = 0; i < repeats; i++)
        {
            reads.Add(db.BeginReadOnlyBatch());

            using var block = db.BeginNextBatch();
            block.SetAccount(account, value);
            await block.Commit(CommitOptions.FlushDataAndRoot);
        }

        foreach (var read in reads)
        {
            read.Dispose();
        }
    }

    [Test]
    [Category(Categories.LongRunning)]
    public async Task Reuse_in_grow_and_shrink()
    {
        const int repeats = 200_000;
        const int spikeEvery = 100;
        const int spikeSize = 1000;

        var account = Keccak.EmptyTreeHash;
        var spikeAccounts = new Keccak[spikeSize];

        new Random(13).NextBytes(MemoryMarshal.Cast<Keccak, byte>(spikeAccounts.AsSpan()));

        byte[] value = [13];

        using var db = PagedDb.NativeMemoryDb(2048 * Page.PageSize);

        for (var i = 0; i < repeats; i++)
        {
            using var block = db.BeginNextBatch();
            block.SetAccount(account, value);

            if (i % spikeEvery == 0)
            {
                // spike set
                foreach (var spikeAccount in spikeAccounts)
                {
                    block.SetAccount(spikeAccount, value);
                }
            }
            else if (i % spikeEvery == 1)
            {
                // spike delete
                foreach (var spikeAccount in spikeAccounts)
                {
                    block.SetAccount(spikeAccount, ReadOnlySpan<byte>.Empty);
                }
            }

            await block.Commit(CommitOptions.FlushDataAndRoot);
        }
    }
}