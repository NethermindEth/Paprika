﻿using System.Buffers.Binary;
using NUnit.Framework;
using Paprika.Crypto;
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

        var view = new TreeView();
        db.VisitRoot(view);

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