using FluentAssertions;
using NUnit.Framework;
using Paprika.Chain;
using Paprika.Crypto;
using Paprika.Data;
using Paprika.Merkle;
using Paprika.Store;

namespace Paprika.Tests.Chain;

public class RawTests
{
    [Test]
    public async Task Raw_access_spin()
    {
        var account = Values.Key1;

        using var db = PagedDb.NativeMemoryDb(256 * 1024, 2);
        var merkle = new ComputeMerkleBehavior();

        await using var blockchain = new Blockchain(db, merkle);

        using var raw = blockchain.StartRaw();

        for (uint i = 0; i < 10_000; i++)
        {
            raw.SetAccount(account, new Account(i, i));
        }

        raw.DestroyAccount(account);
        raw.Commit();

        raw.Hash.Should().Be(Keccak.EmptyTreeHash);
    }

    [Test]
    public async Task Snap_boundary()
    {
        var account = Values.Key1;
        var keccak = Values.Key2;

        using var db = PagedDb.NativeMemoryDb(256 * 1024, 2);
        var merkle = new ComputeMerkleBehavior();

        await using var blockchain = new Blockchain(db, merkle);
        using var raw = blockchain.StartRaw();

        raw.SetBoundary(NibblePath.FromKey(account).SliceTo(1), keccak);
        raw.Commit();

        var root1 = raw.Hash;

        raw.SetAccount(account, new Account(1, 1));
        raw.Commit();

        var root2 = raw.Hash;

        root1.Should().NotBe(root2);
    }

    [Test]
    public async Task Disposal()
    {
        var account = Values.Key1;

        using var db = PagedDb.NativeMemoryDb(256 * 1024, 2);
        var merkle = new ComputeMerkleBehavior();

        await using var blockchain = new Blockchain(db, merkle);

        using var raw = blockchain.StartRaw();

        for (uint i = 0; i < 10_000; i++)
        {
            raw.SetAccount(account, new Account(i, i));
            raw.Commit();
        }

        raw.DestroyAccount(account);
        raw.Commit();

        raw.Hash.Should().Be(Keccak.EmptyTreeHash);
    }
}