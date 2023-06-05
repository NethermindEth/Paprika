using NUnit.Framework;
using Paprika.Crypto;
using Paprika.Store;

namespace Paprika.Tests;

public class MerkleTests
{
    private const int SmallDb = 256 * Page.PageSize;

    [Test]
    public void Empty_database()
    {
        using var db = PagedDb.NativeMemoryDb(SmallDb);

        using (var batch = db.BeginNextBatch())
        {
            batch.Commit(CommitOptions.FlushDataAndRoot);
        }

        using (var batch = db.BeginReadOnlyBatch())
        {
            var rootHash = batch.GetRootHash();
            var expectedRootHash =
                new Keccak(Convert.FromHexString("56E81F171BCC55A6FF8345E692C0F86E5B48E01B996CADC001622FB5E363B421"));

            Assert.That(rootHash, Is.EqualTo(expectedRootHash));
        }
    }

    [Test]
    public void Single_account()
    {
        using var db = PagedDb.NativeMemoryDb(SmallDb);

        var key = Values.Key0;
        var account = new Account(Values.Balance0, Values.Nonce0);

        using (var batch = db.BeginNextBatch())
        {
            batch.Set(key, account);
            batch.Commit(CommitOptions.FlushDataAndRoot);
        }

        using (var batch = db.BeginReadOnlyBatch())
        {
            var rootHash = batch.GetRootHash();
            var expectedRootHash =
                new Keccak(Convert.FromHexString("E2533A0A0C4F1DDB72FEB7BFAAD12A83853447DEAAB6F28FA5C443DD2D37C3FB"));

            Assert.That(rootHash, Is.EqualTo(expectedRootHash));
        }
    }
}
