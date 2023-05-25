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

    private static readonly Keccak Block1a = Build(nameof(Block1a));
    private static readonly Keccak Block1b = Build(nameof(Block1b));

    [Test]
    public void Simple()
    {
        using var db = PagedDb.NativeMemoryDb(16 * Mb, 2);

        using var blockchain = new Blockchain(db);

        var block1a = blockchain.StartNew(Keccak.Zero, Block1a, 1);
        var block1b = blockchain.StartNew(Keccak.Zero, Block1b, 1);

        var account0a = new Account(1, 1);
        var account0b = new Account(2, 2);

        block1a.SetAccount(Key0, account0a);
        block1b.SetAccount(Key0, account0b);

        block1a.GetAccount(Key0).Should().Be(account0a);
        block1b.GetAccount(Key0).Should().Be(account0b);
    }

    private static Keccak Build(string name) => Keccak.Compute(Encoding.UTF8.GetBytes(name));
}