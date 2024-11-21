using System.Buffers.Binary;
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

        using var db = PagedDb.NativeMemoryDb(16 * 1024 * 1024, 2);
        using var merkle = new ComputeMerkleBehavior();

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

        await blockchain.Finalize(hash);

        using var read = blockchain.StartReadOnly(hash);
        var account = read.GetAccount(Key0);
        var recalculatedStorage = merkle.CalculateStorageHash(read, Key0);

        recalculatedStorage.Should().Be(account.StorageRootHash);
    }

    [Explicit]
    [TestCase(false)]
    [TestCase(true)]
    public async Task Account_creation_hint(bool newHint)
    {
        // Create an account with lots of storage slots occupied, then try to create a new account
        // that has almost the same key to collide in the storage.
        const int storageCount = 10_000;
        var random = new Random(17);
        var value = new byte[] { 13, 17, 23, 29 };

        var parent = Keccak.EmptyTreeHash;
        uint parentNumber = 1;

        var addr = Keccak.Zero;

        using var db = PagedDb.NativeMemoryDb(256 * 1024 * 1024, 2);
        var merkle = new ComputeMerkleBehavior();

        await using var blockchain = new Blockchain(db, merkle);

        using var block1 = blockchain.StartNew(parent);
        block1.SetAccount(addr, new Account(1, 1));
        for (var i = 0; i < storageCount; i++)
        {
            block1.SetStorage(addr, random.NextKeccak(), value);
        }

        parent = block1.Commit(parentNumber);
        blockchain.Finalize(parent);

        await blockchain.WaitTillFlush(parent);

        var b = blockchain.StartNew(parent);

        const int every = 1000;

        // Create 64k account to collide on the storage as much as possible
        for (uint i = 1; i <= 60001; i++)
        {
            BinaryPrimitives.WriteUInt32LittleEndian(addr.BytesAsSpan.Slice(28), i);

            b.SetAccount(addr, new Account(1, 1), newHint);
            b.SetStorage(addr, addr, value);

            if (i % every == 0)
            {
                parent = b.Commit(parentNumber + 1);
                parentNumber++;
                b.Dispose();

                blockchain.Finalize(parent);

                b = blockchain.StartNew(parent);
            }
        }

        // commit final
        parent = b.Commit(parentNumber + 1);
        b.Dispose();
        blockchain.Finalize(parent);

        await blockchain.WaitTillFlush(parent);
    }

    [Explicit]
    [Test(Description = "Testing the hint that precommit can use to remember what was destroyed")]
    public async Task Accounts_recreation()
    {
        const int count = 100;
        const int spins = 500;
        const int seed = 17;

        var random = new Random(seed);
        var value1 = new byte[] { 13, 17, 23, 29 };
        var value2 = new byte[] { 13, 17, 23, 17 };

        var parent = Keccak.EmptyTreeHash;
        uint parentNumber = 1;

        using var db = PagedDb.NativeMemoryDb(256 * 1024 * 1024, 2);
        var merkle = new ComputeMerkleBehavior();

        await using var blockchain = new Blockchain(db, merkle);

        using var block1 = blockchain.StartNew(parent);
        for (var i = 0; i < count; i++)
        {
            var keccak = random.NextKeccak();

            block1.SetAccount(keccak, new Account(1, 1));
            block1.SetStorage(keccak, keccak, value1);
        }

        parent = block1.Commit(parentNumber);
        parentNumber++;

        blockchain.Finalize(parent);

        await blockchain.WaitTillFlush(parent);

        // destroy all but one
        for (uint spin = 0; spin < spins; spin++)
        {
            random = new Random(seed);
            using var block = blockchain.StartNew(parent);

            for (var i = 0; i < count; i++)
            {
                var keccak = random.NextKeccak();

                // recreate
                block.DestroyAccount(keccak);
                block.SetAccount(keccak, new Account(spin + 2, spin + 2));
                block.SetStorage(keccak, keccak, value2);
            }

            parent = block.Commit(parentNumber);
            parentNumber++;

            blockchain.Finalize(parent);
        }

        await blockchain.WaitTillFlush(parent);
    }
}
