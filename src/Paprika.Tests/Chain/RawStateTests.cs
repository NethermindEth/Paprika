using FluentAssertions;
using Nethermind.Int256;
using NUnit.Framework;
using Paprika.Chain;
using Paprika.Crypto;
using Paprika.Data;
using Paprika.Merkle;
using Paprika.Store;

namespace Paprika.Tests.Chain;

public class RawStateTests
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

        raw.Finalize(1);

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

        raw.Finalize(1);

        var root2 = raw.Hash;

        root1.Should().NotBe(root2);
    }

    [Test]
    public async Task Metadata_are_preserved()
    {
        var a = Values.Key1;
        var b = Values.Key2;
        var c = Values.Key3;

        UInt256 valueA = 1;
        UInt256 valueB = 1;
        UInt256 valueC = 1;

        using var db = PagedDb.NativeMemoryDb(256 * 1024, 2);
        var merkle = new ComputeMerkleBehavior();

        await using var blockchain = new Blockchain(db, merkle);

        using var raw = blockchain.StartRaw();
        raw.SetAccount(a, new Account(valueA, valueA));
        raw.Commit();

        raw.SetAccount(b, new Account(valueB, valueB));
        raw.Commit();

        raw.SetAccount(c, new Account(valueC, valueC));
        raw.Commit();

        var root = raw.Hash;

        raw.Finalize(1);

        using var read = db.BeginReadOnlyBatch(root);

        read.Metadata.StateHash.Should().Be(root);
        read.GetAccount(a).Should().Be(new Account(valueA, valueA));
        read.GetAccount(b).Should().Be(new Account(valueB, valueB));
        read.GetAccount(c).Should().Be(new Account(valueC, valueC));
    }

    [Test]
    public async Task Disposal()
    {
        var account = Values.Key1;

        using var db = PagedDb.NativeMemoryDb(256 * 1024, 2);
        var merkle = new ComputeMerkleBehavior();

        await using var blockchain = new Blockchain(db, merkle);

        using var raw = blockchain.StartRaw();

        for (uint i = 0; i < 1_000; i++)
        {
            raw.SetAccount(account, new Account(i, i));
            raw.Commit();
        }

        raw.DestroyAccount(account);
        raw.Commit();

        raw.Finalize(1);

        raw.Hash.Should().Be(Keccak.EmptyTreeHash);
    }

    [Test]
    public async Task DeleteByPrefix()
    {
        var account = Values.Key1;

        using var db = PagedDb.NativeMemoryDb(256 * 1024, 2);
        var merkle = new ComputeMerkleBehavior();

        await using var blockchain = new Blockchain(db, merkle);

        using var raw = blockchain.StartRaw();

        raw.SetAccount(account, new Account(1, 1));
        raw.Commit();

        raw.RegisterDeleteByPrefix(Key.Account(account));
        raw.Commit();

        raw.Finalize(1);

        using var read = db.BeginReadOnlyBatch();
        read.TryGet(Key.Account(account), out _).Should().BeFalse();
    }

    [Test]
    public async Task DeleteByShortPrefix()
    {
        var account1 = new Keccak(new byte[]
            { 1, 1, 2, 3, 4, 5, 6, 7, 0, 1, 2, 3, 4, 5, 6, 7, 0, 1, 2, 3, 4, 5, 6, 7, 0, 1, 2, 3, 4, 5, 6, 7, });

        var account2 = new Keccak(new byte[]
            { 18, 1, 2, 3, 4, 5, 6, 7, 0, 1, 2, 3, 4, 5, 6, 7, 0, 1, 2, 3, 4, 5, 6, 7, 0, 1, 2, 3, 4, 5, 6, 7, });

        using var db = PagedDb.NativeMemoryDb(64 * 1024, 2);
        var merkle = new ComputeMerkleBehavior();

        await using var blockchain = new Blockchain(db, merkle);

        using var raw = blockchain.StartRaw();

        raw.SetAccount(account1, new Account(1, 1));
        raw.SetAccount(account2, new Account(2, 2));
        raw.Commit();

        raw.RegisterDeleteByPrefix(Key.Account(NibblePath.FromKey(account2).SliceTo(1)));
        raw.Commit();

        raw.Finalize(1);

        //check account 1 is still present and account 2 is deleted
        using var read = db.BeginReadOnlyBatch();
        read.TryGet(Key.Account(account1), out _).Should().BeTrue();
        read.TryGet(Key.Account(account2), out _).Should().BeFalse();

        //let's re-add 2nd account and delete using empty prefix
        using var raw2 = blockchain.StartRaw();

        raw2.SetAccount(account2, new Account(2, 2));
        raw2.Commit();

        raw2.RegisterDeleteByPrefix(Key.Account(NibblePath.Empty));
        raw2.Commit();

        raw2.Finalize(2);

        //no accounts should be present
        using var read2 = db.BeginReadOnlyBatch();
        read2.TryGet(Key.Account(account1), out _).Should().BeFalse();
        read2.TryGet(Key.Account(account2), out _).Should().BeFalse();
    }
}
