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

        using var head = multi.Begin(Keccak.Zero);

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

        head.Commit(1, Keccak.EmptyTreeHash);

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
    public async Task Multiple_blocks()
    {
        const byte blocks = 64;

        using var db = PagedDb.NativeMemoryDb(1 * Mb, 2);

        var random = new Random(Seed);

        await using var multi = db.OpenMultiHeadChain();
        using var head = multi.Begin(Keccak.Zero);

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
}