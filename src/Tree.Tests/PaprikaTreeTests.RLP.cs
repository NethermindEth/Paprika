using System.Globalization;
using NUnit.Framework;

namespace Tree.Tests;

public class PaprikaTreeTestsRlp
{
    [Test]
    public void Leaf_Short_To_RLP()
    {
        var key = NibblePath.FromKey(stackalloc byte[] { 0x12, 0x34 });
        Span<byte> value = stackalloc byte[] { 3, 5, 7, 11 };
        var expected = new byte[] { 201, 131, 32, 18, 52, 132, 3, 5, 7, 11 };

        AssertLeaf(expected, key, value);
    }

    [Test]
    public void Leaf_Long_To_Keccak()
    {
        var key = NibblePath.FromKey(stackalloc byte[] { 0x12, 0x34 });
        var value = new byte[32];

        var keccak = ParseHex("0xc9a263dc573d67a8d0627756d012385a27db78bb4a072ab0f755a84d3b4babda");

        AssertLeaf(keccak, key, value);
    }

    [Test]
    public void Extension_Short_To_RLP()
    {
        // leaf
        Span<byte> leaf = stackalloc byte[32];
        PaprikaTree.EncodeLeaf(NibblePath.FromKey(stackalloc byte[] { 0x03 }).SliceFrom(1), stackalloc byte[] { 0x05 },
            leaf);
        var leafRlp = leaf.Slice(1, leaf[0]);

        // extension 
        var path = NibblePath.FromKey(stackalloc byte[] { 0x07 }).SliceFrom(1);
        AssertExtension(new byte[] { 196, 23, 194, 51, 5 }, path, leafRlp);
    }

    [Test]
    public void Extension_Long_To_Keccak()
    {
        // leaf
        var key = NibblePath.FromKey(stackalloc byte[] { 0x12, 0x34 });
        var value = new byte[32];
        Span<byte> keccak = stackalloc byte[32];
        PaprikaTree.EncodeLeaf(key, value, keccak);

        // extension 
        var path = NibblePath.FromKey(stackalloc byte[] { 0x07 }).SliceFrom(1);

        var expected = ParseHex("0x87096a8380f2003182a4fa0409326e6678e0c5cf55418fc0aa516ae06b66be46");

        AssertExtension(expected, path, keccak);
    }

    [Test]
    public void Branch_Long_To_Keccak()
    {
        var db = new MemoryDb(1024);
        var store = new Store(db);

        // leaf
        var key = NibblePath.FromKey(stackalloc byte[] { 0x12, 0x34 });
        var value = new byte[32];

        var leaf = PaprikaTree.WriteLeaf(store, key.SliceFrom(0), value);

        Span<byte> branch = stackalloc byte[PaprikaTree.Branch.GetNeededSize(1)];
        branch.Clear();
        branch[0] = PaprikaTree.BranchType;
        PaprikaTree.Branch.SetNonExistingYet(branch, 11, leaf);

        var branchId = store.Write(branch);

        var expected = ParseHex("0xfe8ac9a9c96e07c71fdb2c4cda32cb86e4e880616cf4dad1454247c28dd3a739");

        AssertBranch(expected, branchId, db);
    }

    private static void AssertLeaf(byte[] expected, in NibblePath path, in ReadOnlySpan<byte> value)
    {
        Span<byte> destination = stackalloc byte[32];
        var encoded = PaprikaTree.EncodeLeaf(path, value, destination);
        AssertEncoded(expected, encoded, destination);
    }

    private static void AssertExtension(byte[] expected, in NibblePath path, in ReadOnlySpan<byte> childRlpOrKeccak)
    {
        Span<byte> destination = stackalloc byte[32];
        var encoded = PaprikaTree.EncodeExtension(path, childRlpOrKeccak, destination);
        AssertEncoded(expected, encoded, destination);
    }

    private static void AssertBranch(byte[] expected, long branchId, IDb db)
    {
        Span<byte> destination = stackalloc byte[32];
        var encoded = PaprikaTree.EncodeBranch(db.Read(branchId), db, destination);
        AssertEncoded(expected, encoded, destination);
    }

    private static void AssertEncoded(byte[] expected, PaprikaTree.KeccakOrRlp encoded, Span<byte> destination)
    {
        if (encoded == PaprikaTree.KeccakOrRlp.Keccak)
        {
            // keccak
            CollectionAssert.AreEqual(expected, destination.ToArray());
        }
        else
        {
            // rlp
            var length = destination[0];
            var rlp = destination.Slice(1, length);
            CollectionAssert.AreEqual(expected, rlp.ToArray());
        }
    }


    private static byte[] ParseHex(string hex)
    {
        hex = hex.Replace("0x", "");
        var result = new byte[hex.Length / 2];

        for (int i = 0; i < hex.Length; i += 2)
        {
            result[i / 2] = byte.Parse(hex.Substring(i, 2), NumberStyles.HexNumber);
        }

        return result;
    }

    class Store : PaprikaTree.IStore
    {
        private readonly IDb _db;

        public Store(IDb db)
        {
            _db = db;
        }

        public long TryUpdateOrAdd(long current, in Span<byte> written)
        {
            throw new NotImplementedException();
        }

        public ReadOnlySpan<byte> Read(long id)
        {
            throw new NotImplementedException();
        }

        public long Write(ReadOnlySpan<byte> payload) => _db.Write(payload);
    }
}