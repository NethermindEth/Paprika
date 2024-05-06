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

    [Test]
    public async Task Reuse_in_limited_environment()
    {
        const int repeats = 10_000;
        var account = Keccak.EmptyTreeHash;

        byte[] value = [13];

        using var db = PagedDb.NativeMemoryDb(24 * Page.PageSize);

        for (var i = 0; i < repeats; i++)
        {
            using var block = db.BeginNextBatch();
            block.SetAccount(account, value);
            await block.Commit(CommitOptions.FlushDataAndRoot);
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

        using var db = PagedDb.NativeMemoryDb(1024 * Page.PageSize);

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
