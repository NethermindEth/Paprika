using System.Buffers.Binary;
using System.Text;
using FluentAssertions;
using Nethermind.Int256;
using NUnit.Framework;
using Paprika.Chain;
using Paprika.Crypto;
using Paprika.Store;
using static Paprika.Tests.Values;

namespace Paprika.Tests.Chain;

public class BlockchainTests
{
    private const int Mb = 1024 * 1024;

    private static readonly Keccak Block1A = Build(nameof(Block1A));
    private static readonly Keccak Block1B = Build(nameof(Block1B));

    private static readonly Keccak Block2A = Build(nameof(Block2A));

    private static readonly Keccak Block3A = Build(nameof(Block3A));

    [Test]
    public async Task Simple()
    {
        var account1A = new Account(1, 1);
        var account1B = new Account(2, 2);

        using var db = PagedDb.NativeMemoryDb(16 * Mb, 2);

        await using var blockchain = new Blockchain(db);

        using (var block1A = blockchain.StartNew(Keccak.Zero, Block1A, 1))
        using (var block1B = blockchain.StartNew(Keccak.Zero, Block1B, 1))
        {
            block1A.SetAccount(Key0, account1A);
            block1B.SetAccount(Key0, account1B);

            block1A.GetAccount(Key0).Should().Be(account1A);
            block1B.GetAccount(Key0).Should().Be(account1B);

            // commit both blocks as they were seen in the network
            block1A.Commit();
            block1B.Commit();

            // start a next block
            using var block2A = blockchain.StartNew(Block1A, Block2A, 2);

            // assert whether the history is preserved
            block2A.GetAccount(Key0).Should().Be(account1A);
            block2A.Commit();
        }

        // finalize second block
        blockchain.Finalize(Block2A);

        // for now, to monitor the block chain, requires better handling of ref-counting on finalized
        await Task.Delay(1000);

        // start the third block
        using var block3A = blockchain.StartNew(Block2A, Block3A, 3);
        block3A.Commit();

        block3A.GetAccount(Key0).Should().Be(account1A);
    }

    [Test]
    public async Task BiggerTest()
    {
        const int blockCount = 10;
        const int perBlock = 1000;

        using var db = PagedDb.NativeMemoryDb(128 * Mb, 2);

        await using var blockchain = new Blockchain(db);

        var counter = 0;

        var previousBlock = Keccak.Zero;

        for (var i = 1; i < blockCount + 1; i++)
        {
            var hash = BuildKey(i);

            using var block = blockchain.StartNew(previousBlock, hash, (uint)i);

            for (var j = 0; j < perBlock; j++)
            {
                var key = BuildKey(counter);

                block.SetAccount(key, GetAccount(counter));
                block.SetStorage(key, key, ((UInt256)counter).ToBigEndian());

                counter++;
            }

            // commit first
            block.Commit();

            if (i > 1)
            {
                blockchain.Finalize(previousBlock);
            }

            previousBlock = hash;
        }

        // make next visible
        using var next = blockchain.StartNew(previousBlock, BuildKey(blockCount + 1), (uint)blockCount + 1);
        next.Commit();

        blockchain.Finalize(previousBlock);

        // for now, to monitor the block chain, requires better handling of ref-counting on finalized
        await Task.Delay(1000);

        using var read = db.BeginReadOnlyBatch();

        read.Metadata.BlockNumber.Should().Be(blockCount);

        // reset the counter
        counter = 0;
        for (int i = 1; i < blockCount + 1; i++)
        {
            for (var j = 0; j < perBlock; j++)
            {
                var key = BuildKey(counter);

                read.ShouldHaveAccount(key, GetAccount(counter));
                read.ShouldHaveStorage(key, key, ((UInt256)counter).ToBigEndian());

                counter++;
            }
        }
    }

    private static Account GetAccount(int i) => new((UInt256)i, (UInt256)i);

    private static Keccak BuildKey(int i)
    {
        Span<byte> span = stackalloc byte[4];
        BinaryPrimitives.WriteInt32LittleEndian(span, i);
        return Keccak.Compute(span);
    }

    private static Keccak Build(string name) => Keccak.Compute(Encoding.UTF8.GetBytes(name));
}