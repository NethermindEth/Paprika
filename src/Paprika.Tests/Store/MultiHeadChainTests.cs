using System.Buffers.Binary;
using FluentAssertions;
using Paprika.Crypto;
using Paprika.Data;
using Paprika.Store;

namespace Paprika.Tests.Store;

public class MultiHeadChainTests
{
    private const long Mb = 1024 * 1024;
    private const int Seed = 17;

    [Test]
    public async Task SimpleTest()
    {
        const int accounts = 1;
        const int merkleCount = 31;

        ushort counter = 0;

        using var db = PagedDb.NativeMemoryDb(1 * Mb, 2);

        var random = new Random(Seed);

        await using var multi = db.OpenMultiHeadChain();

        using var head = multi.Begin(Keccak.EmptyTreeHash);

        for (var i = 0; i < accounts; i++)
        {
            var keccak = random.NextKeccak();

            // account first & data
            head.SetRaw(Key.Account(keccak), GetData());
            head.SetRaw(Key.StorageCell(NibblePath.FromKey(keccak), keccak), GetData());

            for (var j = 0; j < merkleCount; j++)
            {
                // all the Merkle values
                head.SetRaw(Key.Merkle(NibblePath.FromKey(keccak).SliceTo(j)), GetData());
            }
        }

        var someOtherKeccak = Keccak.OfAnEmptyString;
        head.Commit(1, someOtherKeccak);

        // reset
        counter = 0;
        random = new Random(Seed);

        Assert();

        void Assert()
        {
            for (var i = 0; i < accounts; i++)
            {
                var keccak = random.NextKeccak();

                head.TryGet(Key.Account(keccak), out var value).Should().BeTrue("The account should exist");
                value.SequenceEqual(GetData()).Should().BeTrue("The account should have data right");

                head.TryGet(Key.StorageCell(NibblePath.FromKey(keccak), keccak), out value).Should()
                    .BeTrue("The storage cell should exist");
                value.SequenceEqual(GetData()).Should().BeTrue("The storage cell should have data right");

                for (var j = 0; j < merkleCount; j++)
                {
                    // all the Merkle values
                    head.TryGet(Key.Merkle(NibblePath.FromKey(keccak).SliceTo(j)), out value).Should()
                        .BeTrue("The Merkle should exist");

                    var actual = value.ToArray();
                    var expected = GetData();

                    actual.SequenceEqual(expected).Should()
                        .BeTrue($"The Merkle @{j} of {i}th account should have data right");
                }
            }
        }

        byte[] GetData()
        {
            var bytes = new byte[2];
            BinaryPrimitives.WriteUInt16LittleEndian(bytes, counter);
            counter++;
            return bytes;
        }
    }

    [Test]
    public async Task Multiple_blocks_read_not_finalized()
    {
        const byte blocks = 64;

        using var db = PagedDb.NativeMemoryDb(1 * Mb, 2);

        var random = new Random(Seed);

        await using var multi = db.OpenMultiHeadChain();
        using var head = multi.Begin(Keccak.EmptyTreeHash);

        for (byte i = 0; i < blocks; i++)
        {
            var keccak = random.NextKeccak();
            head.SetRaw(Key.Account(keccak), [i]);
            head.Commit((uint)(i + 1), keccak);
        }

        Assert();

        void Assert()
        {
            // Clear
            random = new Random(Seed);

            for (byte i = 0; i < blocks; i++)
            {
                var keccak = random.NextKeccak();
                head.TryGet(Key.Account(keccak), out var data)
                    .Should().BeTrue("The account should exist");
                data.SequenceEqual([i]).Should().BeTrue();
            }
        }
    }

    [TestCase(true, TestName = "Finalize before read")]
    [TestCase(false, TestName = "Read from proposed")]
    public async Task Multiple_blocks(bool finalize)
    {
        const byte blocks = 64;

        using var db = PagedDb.NativeMemoryDb(1 * Mb, 2);

        var random = new Random(Seed);

        await using var multi = db.OpenMultiHeadChain();
        using var head = multi.Begin(Keccak.EmptyTreeHash);

        Keccak root = default;
        for (byte i = 0; i < blocks; i++)
        {
            root = random.NextKeccak();

            head.SetRaw(Key.Account(root), [i]);
            head.Commit((uint)(i + 1), root);
        }

        // Finalization should not impact the reads.
        // If it's finalized, we read from the recent root.
        // If it's not, from blocks in memory.
        if (finalize)
        {
            await multi.Finalize(root);
        }

        multi.TryLeaseReader(root, out var r);
        using var read = r;

        Assert();

        void Assert()
        {
            // Clear
            random = new Random(Seed);

            for (byte i = 0; i < blocks; i++)
            {
                var keccak = random.NextKeccak();
                read.TryGet(Key.Account(keccak), out var data)
                    .Should().BeTrue("The account should exist");
                data.SequenceEqual([i]).Should().BeTrue();
            }
        }
    }

    [Test]
    public async Task Old_reader_kept_alive_keeps_finalization_from_processing()
    {
        const byte blocks = 64;

        using var db = PagedDb.NativeMemoryDb(1 * Mb, 2);

        var random = new Random(Seed);

        await using var multi = db.OpenMultiHeadChain();

        using var head = multi.Begin(Keccak.EmptyTreeHash);

        var finalized = new List<Task>();

        var account = Keccak.OfAnEmptySequenceRlp;

        byte[] expectedReaderValue = [];
        byte[] lastWrittenValue = [];
        Keccak lastWrittenKeccak = default;

        IHeadReader? reader = null;
        for (byte i = 0; i < blocks; i++)
        {
            lastWrittenKeccak = random.NextKeccak();

            lastWrittenValue = [i];
            head.SetRaw(Key.Account(account), lastWrittenValue);
            head.Commit((uint)(i + 1), lastWrittenKeccak);

            if (i == 0)
            {
                expectedReaderValue = lastWrittenValue;

                // the first block should be set and finalized
                await multi.Finalize(lastWrittenKeccak);
                multi.TryLeaseReader(lastWrittenKeccak, out reader).Should().BeTrue();
            }
            else
            {
                // Just register finalization, it won't be finalized as the reader will keep it from going on 
                finalized.Add(multi.Finalize(lastWrittenKeccak));
            }
        }

        AssertReader(reader, account, expectedReaderValue);

        finalized.Should().AllSatisfy(t => t.Status.Should().Be(TaskStatus.WaitingForActivation));

        reader!.Dispose();
        await reader.CleanedUp;
        await Task.WhenAll(finalized);

        // Everything is finalized and written, try read the last value now
        AssertLastWrittenValue(multi, lastWrittenKeccak, account, lastWrittenValue);

        return;

        static void AssertReader(IHeadReader? reader, Keccak account, byte[] expectedReaderValue)
        {
            reader.Should().NotBeNull();

            reader!.TryGet(Key.Account(account), out var actualReaderValue).Should().BeTrue();
            actualReaderValue.SequenceEqual(expectedReaderValue).Should().BeTrue();
        }

        static void AssertLastWrittenValue(IMultiHeadChain multi, Keccak stateHash, Keccak account, byte[] lastWrittenValue)
        {
            multi.TryLeaseReader(stateHash, out var lastReader).Should().BeTrue();
            lastReader.TryGet(Key.Account(account), out var actualLastWrittenValue).Should().BeTrue();
            actualLastWrittenValue.SequenceEqual(lastWrittenValue).Should().BeTrue();
            lastReader.Dispose();
        }
    }

    [Test]
    public async Task Last_finalized_can_be_leased_with_LeaseLatest()
    {
        const byte blocks = 128;

        using var db = PagedDb.NativeMemoryDb(1 * Mb, 2);

        var random = new Random(Seed);

        await using var multi = db.OpenMultiHeadChain();
        using var head = multi.Begin(Keccak.EmptyTreeHash);

        for (byte i = 0; i < blocks; i++)
        {
            var keccak = random.NextKeccak();
            byte[] expected = [i];
            head.SetRaw(Key.Account(keccak), expected);
            head.Commit((uint)(i + 1), keccak);

            await multi.Finalize(keccak);

            AssertLatest(multi, keccak, expected);
        }

        return;

        static void AssertLatest(IMultiHeadChain multi, Keccak keccak, byte[] expected)
        {
            using var reader = multi.LeaseLatestFinalized();
            reader.Metadata.StateHash.Should().Be(keccak, "Because it should lease the latest");
            reader.TryGet(Key.Account(keccak), out var actual).Should().BeTrue();
            actual.SequenceEqual(expected).Should().BeTrue();
        }
    }
}