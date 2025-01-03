using FluentAssertions;
using Nethermind.Int256;
using Paprika.Chain;
using Paprika.Crypto;
using Paprika.Data;
using Paprika.Merkle;
using Paprika.Store;
using Paprika.RLP;

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
        raw.Commit(keepOpened: true);

        raw.Finalize(1);

        raw.Hash.Should().Be(Keccak.EmptyTreeHash);
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
        raw.Commit(keepOpened: true);

        raw.SetAccount(b, new Account(valueB, valueB));
        raw.Commit(keepOpened: true);

        raw.SetAccount(c, new Account(valueC, valueC));
        raw.Commit(keepOpened: true);

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
            raw.Commit(keepOpened: true);
        }

        raw.DestroyAccount(account);
        raw.Commit(keepOpened: true);

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
        raw.Commit(keepOpened: true);

        raw.RegisterDeleteByPrefix(Key.Account(account));
        raw.Commit(keepOpened: true);

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
        raw.Commit(keepOpened: true);

        raw.RegisterDeleteByPrefix(Key.Account(NibblePath.FromKey(account2).SliceTo(1)));
        raw.Commit(keepOpened: true);

        raw.Finalize(1);

        //check account 1 is still present and account 2 is deleted
        using var read = db.BeginReadOnlyBatch();
        read.TryGet(Key.Account(account1), out _).Should().BeTrue();
        read.TryGet(Key.Account(account2), out _).Should().BeFalse();

        //let's re-add 2nd account and delete using empty prefix
        using var raw2 = blockchain.StartRaw();

        raw2.SetAccount(account2, new Account(2, 2));
        raw2.Commit(keepOpened: true);

        raw2.RegisterDeleteByPrefix(Key.Account(NibblePath.Empty));
        raw2.Commit(keepOpened: true);

        raw2.Finalize(2);

        //no accounts should be present
        using var read2 = db.BeginReadOnlyBatch();
        read2.TryGet(Key.Account(account1), out _).Should().BeFalse();
        read2.TryGet(Key.Account(account2), out _).Should().BeFalse();
    }

    [Test]
    public void DeleteByPrefixStorage()
    {
        var account = Values.Key1;

        using var db = PagedDb.NativeMemoryDb(256 * 1024, 2);
        var merkle = new ComputeMerkleBehavior();

        var blockchain = new Blockchain(db, merkle);

        using var raw = blockchain.StartRaw();

        raw.SetAccount(account, new Account(1, 1));
        raw.SetStorage(account, Values.Key2, new byte[] { 1, 2, 3, 4, 5 });
        raw.Commit(keepOpened: true);

        using var read = db.BeginReadOnlyBatch();
        read.TryGet(Key.StorageCell(NibblePath.FromKey(account), Values.Key2), out _).Should().BeTrue();

        raw.RegisterDeleteByPrefix(Key.StorageCell(NibblePath.FromKey(account), NibblePath.Empty));
        raw.Commit(keepOpened: true);

        raw.Finalize(1);

        using var read2 = db.BeginReadOnlyBatch();
        read2.TryGet(Key.StorageCell(NibblePath.FromKey(account), Values.Key2), out _).Should().BeFalse();
    }

    [Test]
    public void CalcRootFromRlpMemoDataState()
    {
        using var db = PagedDb.NativeMemoryDb(256 * 1024, 2);
        var merkle = new ComputeMerkleBehavior();

        var blockchain = new Blockchain(db, merkle);

        using var raw = blockchain.StartRaw();

        var random = GetRandom();

        Span<byte> rlp = stackalloc byte[1024];
        RlpStream stream = new RlpStream(rlp);
        stream.StartSequence(529);

        //all children to trigger parallel branch hash calculation
        byte[] children = new byte[16];
        Keccak[] childHashes = new Keccak[16];

        for (int i = 0; i < 16; i++)
        {
            children[i] = (byte)i;
            childHashes[i] = random.NextKeccak();
            stream.Encode(childHashes[i]);
        }
        stream.EncodeEmptyArray();
        KeccakOrRlp.FromSpan(rlp.Slice(0, stream.Position), out var checkKeccakOrRlp);

        raw.CreateMerkleBranch(Keccak.Zero, NibblePath.Empty, children, childHashes);
        Keccak newRootHash = raw.RefreshRootHash(true);

        newRootHash.Should().Be(checkKeccakOrRlp.Keccak);
    }

    [Test]
    public void CalcRootFromRlpMemoDataStorage()
    {
        using var db = PagedDb.NativeMemoryDb(256 * 1024, 2);
        var merkle = new ComputeMerkleBehavior();

        var blockchain = new Blockchain(db, merkle);

        using var raw = blockchain.StartRaw();

        var account = Values.Key1;

        var random = GetRandom();

        Span<byte> rlp = stackalloc byte[1024];
        RlpStream stream = new RlpStream(rlp);
        stream.StartSequence(529);

        //all children to trigger parallel branch hash calculation
        byte[] children = new byte[16];
        Keccak[] childHashes = new Keccak[16];

        for (int i = 0; i < 16; i++)
        {
            children[i] = (byte)i;
            childHashes[i] = random.NextKeccak();
            stream.Encode(childHashes[i]);
        }
        stream.EncodeEmptyArray();
        KeccakOrRlp.FromSpan(rlp.Slice(0, stream.Position), out var checkKeccakOrRlp);

        raw.CreateMerkleBranch(account, NibblePath.Empty, children, childHashes);
        Keccak newRootHash = raw.RecalculateStorageRoot(account, true);

        newRootHash.Should().Be(checkKeccakOrRlp.Keccak);
    }

    private static Random GetRandom() => new(13);
}
