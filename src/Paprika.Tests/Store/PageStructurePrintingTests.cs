using System.Buffers.Binary;
using NUnit.Framework;
using Paprika.Chain;
using Paprika.Crypto;
using Paprika.Merkle;
using Paprika.Store;
using Spectre.Console;

namespace Paprika.Tests.Store;

[Explicit]
public class PageStructurePrintingTests
{
    private const int SmallDb = 256 * Page.PageSize;
    private const int MB = 1024 * 1024;
    private const int MB16 = 16 * MB;
    private const int MB64 = 64 * MB;
    private const int MB128 = 128 * MB;
    private const int MB256 = 256 * MB;

    [Test]
    public async Task Uniform_buckets_spin()
    {
        var account = Keccak.EmptyTreeHash;

        const int size = MB256;
        using var db = PagedDb.NativeMemoryDb(size);

        const int batches = 5;
        const int storageSlots = 350_000;

        var value = new byte[32];

        var random = new Random(13);
        random.NextBytes(value);

        for (var i = 0; i < batches; i++)
        {
            using var batch = db.BeginNextBatch();

            for (var slot = 0; slot < storageSlots; slot++)
            {
                batch.SetStorage(account, GetStorageAddress(slot), value);
            }

            await batch.Commit(CommitOptions.FlushDataAndRoot);
        }

        var view = new TreeView(db);
        db.VisitRoot(view);

        AnsiConsole.WriteLine($"DB size: {db.Megabytes:N0}MB");
        AnsiConsole.Write(view.Tree);

        return;

        Keccak GetStorageAddress(int i)
        {
            Keccak result = default;
            BinaryPrimitives.WriteInt32LittleEndian(result.BytesAsSpan, i);
            return result;
        }
    }

    [TestCase(400_000)]
    [TestCase(300_000)]
    [TestCase(200_000)]
    public async Task Merkle_one_big_storage_account(int storageSlots)
    {
        var account = Keccak.EmptyTreeHash;

        using var db = PagedDb.NativeMemoryDb(MB256);

        await using var blockchain = new Blockchain(db, new ComputeMerkleBehavior());

        var value = new byte[32];

        const int seed = 13;

        var random = new Random(seed);
        random.NextBytes(value);

        using var block = blockchain.StartNew(Keccak.EmptyTreeHash);

        block.SetAccount(account, new Account(1_000, 1_000, Keccak.OfAnEmptyString, Keccak.OfAnEmptySequenceRlp));

        for (var slot = 0; slot < storageSlots; slot++)
        {
            block.SetStorage(account, GetStorageAddress(slot), value);
        }

        var commit = block.Commit(1);
        await blockchain.Finalize(commit);

        // Assert
        using var read = db.BeginReadOnlyBatch();
        for (var slot = 0; slot < storageSlots; slot++)
        {
            read.AssertStorageValue(account, GetStorageAddress(slot), value);
        }

        var view = new TreeView(db);
        db.VisitRoot(view);

        AnsiConsole.WriteLine($"DB size: {db.Megabytes:N0}MB");
        AnsiConsole.Write(view.Tree);

        return;

        Keccak GetStorageAddress(int i)
        {
            Keccak result = default;
            BinaryPrimitives.WriteInt32LittleEndian(result.BytesAsSpan, i);
            return result;
        }
    }
}