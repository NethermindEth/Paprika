using NUnit.Framework;
using Paprika.Chain;
using Paprika.Crypto;
using Paprika.Merkle;
using Paprika.Store;

namespace Paprika.Tests;

public class MerkleTests
{
    private const int SmallDb = 256 * Page.PageSize;
    private readonly Keccak _blockKeccak = Keccak.Compute("block"u8);

    [Test]
    public async Task Empty_database()
    {
        using var db = PagedDb.NativeMemoryDb(SmallDb);
        await using var blockchain = new Blockchain(db, preCommit: new RecomputeMerkle());

        // NOTE: If the block keccak is Keccak.Zero, we get a page exception
        using var block = blockchain.StartNew(Keccak.Zero, _blockKeccak, 1);

        block.Commit();

        var rootHash = block.GetMerkleRootHash();
        var expectedRootHash =
            new Keccak(Convert.FromHexString("56E81F171BCC55A6FF8345E692C0F86E5B48E01B996CADC001622FB5E363B421"));

        Assert.That(rootHash, Is.EqualTo(expectedRootHash));
    }

    [Test]
    public async Task Single_account()
    {
        using var db = PagedDb.NativeMemoryDb(SmallDb);
        await using var blockchain = new Blockchain(db, preCommit: new RecomputeMerkle());

        var key = Values.Key0;
        var account = new Account(Values.Balance0, Values.Nonce0);

        using var block = blockchain.StartNew(Keccak.Zero, _blockKeccak, 1);

        block.SetAccount(key, account);
        block.Commit();

        var rootHash = block.GetMerkleRootHash();
        var expectedRootHash =
            new Keccak(Convert.FromHexString("E2533A0A0C4F1DDB72FEB7BFAAD12A83853447DEAAB6F28FA5C443DD2D37C3FB"));

        Assert.That(rootHash, Is.EqualTo(expectedRootHash));
    }
}
