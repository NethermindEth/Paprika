using System.Buffers.Binary;
using FluentAssertions;
using NUnit.Framework;
using Paprika.Data;
using Paprika.Store;

namespace Paprika.Tests.Store;

public class PagedDbTests
{
    private const int Mb = 1024 * 1024;
    private const int Seed = 17;

    [Test]
    public async Task BiggerTest()
    {
        const int accounts = 1;
        const int merkleCount = 31;

        using var db = PagedDb.NativeMemoryDb(256 * Mb, 2);

        var random = new Random(Seed);

        using var batch = db.BeginNextBatch();

        for (var i = 0; i < accounts; i++)
        {
            var keccak = random.NextKeccak();

            // account first & data
            batch.SetRaw(Key.Account(keccak), GetData());
            batch.SetRaw(Key.StorageCell(NibblePath.FromKey(keccak), keccak), GetData());

            for (var j = 0; j < merkleCount; j++)
            {
                // all the Merkle values
                batch.SetRaw(Key.Merkle(NibblePath.FromKey(keccak).SliceTo(j)), GetData());
            }
        }

        await batch.Commit(CommitOptions.FlushDataAndRoot);

        // reset
        random = new Random(Seed);
        _counter = 0;

        Assert();
        return;

        void Assert()
        {
            using var read = db.BeginReadOnlyBatch();

            for (var i = 0; i < accounts; i++)
            {
                var keccak = random.NextKeccak();

                read.TryGet(Key.Account(keccak), out var value).Should().BeTrue("The account should exist");
                value.SequenceEqual(GetData()).Should().BeTrue("The account should have data right");

                read.TryGet(Key.StorageCell(NibblePath.FromKey(keccak), keccak), out value).Should().BeTrue("The storage cell should exist");
                value.SequenceEqual(GetData()).Should().BeTrue("The storage cell should have data right");

                for (var j = 0; j < merkleCount; j++)
                {
                    // all the Merkle values
                    read.TryGet(Key.Merkle(NibblePath.FromKey(keccak).SliceTo(j)), out value).Should().BeTrue("The Merkle should exist");

                    var actual = value.ToArray();
                    var expected = GetData();

                    actual.SequenceEqual(expected).Should().BeTrue($"The Merkle @{j} of {i}th account should have data right");
                }
            }
        }
    }

    private ushort _counter;

    private byte[] GetData()
    {
        BinaryPrimitives.WriteUInt16LittleEndian(bytes, _counter);
        _counter++;
        return bytes;
    }

    private static readonly byte[] bytes = new byte[2];
}