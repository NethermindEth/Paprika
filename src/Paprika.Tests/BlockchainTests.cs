using System.Text;
using FluentAssertions;
using NUnit.Framework;
using Paprika.Chain;
using Paprika.Crypto;
using Paprika.Store;

using static Paprika.Tests.Values;

namespace Paprika.Tests;

public class BlockchainTests
{
    private const int Mb = 1024 * 1024;

    private static readonly Keccak Block1A = Build(nameof(Block1A));
    private static readonly Keccak Block1B = Build(nameof(Block1B));

    private static readonly Keccak Block2A = Build(nameof(Block2A));

    [Test]
    public void Simple()
    {
        using var db = PagedDb.NativeMemoryDb(16 * Mb, 2);

        using var blockchain = new Blockchain(db);

        var block1A = blockchain.StartNew(Keccak.Zero, Block1A, 1);
        var block1B = blockchain.StartNew(Keccak.Zero, Block1B, 1);

        var account1A = new Account(1, 1);
        var account1B = new Account(2, 2);

        block1A.SetAccount(Key0, account1A);
        block1B.SetAccount(Key0, account1B);

        block1A.GetAccount(Key0).Should().Be(account1A);
        block1B.GetAccount(Key0).Should().Be(account1B);

        // commit block 1a as properly processed
        block1A.Commit();

        // dispose block 1b as it was not correct
        block1B.Dispose();

        // start a next block
        var block2a = blockchain.StartNew(Block1A, Block2A, 2);

        // assert whether the history is preserved
        block2a.GetAccount(Key0).Should().Be(account1A);
    }

    private static Keccak Build(string name) => Keccak.Compute(Encoding.UTF8.GetBytes(name));
}