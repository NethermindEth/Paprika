using FluentAssertions;
using NUnit.Framework;
using Paprika.Chain;
using Paprika.Crypto;
using Paprika.Merkle;
using Paprika.Store;
using static Paprika.Tests.Values;

namespace Paprika.Tests.Merkle;

public class AdditionalTests
{
    [Test]
    public async Task Account_destruction_same_block()
    {
        const int seed = 17;
        const int storageCount = 32 * 1024;

        using var db = PagedDb.NativeMemoryDb(4 * 1024 * 1024, 2);
        var merkle = new ComputeMerkleBehavior(2, 2);

        await using var blockchain = new Blockchain(db, merkle);

        using var block1 = blockchain.StartNew(Keccak.EmptyTreeHash);

        var random = new Random(seed);

        block1.SetAccount(Key0, new Account(1, 1));

        for (var i = 0; i < storageCount; i++)
        {
            block1.SetStorage(Key0, random.NextKeccak(), i.ToByteArray());
        }

        const int number = 1;
        var hash = block1.Commit(number);

        blockchain.Finalize(hash);
        await blockchain.WaitTillFlush(number);

        using var read = blockchain.StartReadOnly(hash);
        var account = read.GetAccount(Key0);
        var recalculatedStorage = merkle.CalculateStorageHash(read, Key0);

        recalculatedStorage.Should().Be(account.StorageRootHash);
    }
}