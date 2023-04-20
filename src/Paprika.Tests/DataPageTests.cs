using System.Buffers.Binary;
using FluentAssertions;
using NUnit.Framework;
using Paprika.Pages;
using static Paprika.Tests.Values;

namespace Paprika.Tests;

public class DataPageTests : BasePageTests
{
    private const byte RootLevel = 0;

    const uint BatchId = 1;

    [Test]
    public void Set_then_Get()
    {
        var page = AllocPage();
        page.Clear();

        var batch = NewBatch(BatchId);
        var dataPage = new DataPage(page);
        var ctx = new SetContext(Key0, Balance0, Nonce0, batch);

        var updated = dataPage.Set(ctx, RootLevel);

        new DataPage(updated).GetAccount(Key0, batch, out var account, RootLevel);

        Assert.AreEqual(Nonce0, account.Nonce);
        Assert.AreEqual(Balance0, account.Balance);
    }

    [Test]
    public void Update_key()
    {
        var page = AllocPage();
        page.Clear();

        var batch = NewBatch(BatchId);

        var dataPage = new DataPage(page);
        var ctx1 = new SetContext(Key0, Balance0, Nonce0, batch);
        var ctx2 = new SetContext(Key0, Balance1, Nonce1, batch);

        var updated = dataPage.Set(ctx1, RootLevel);
        updated = new DataPage(updated).Set(ctx2, RootLevel);

        new DataPage(updated).GetAccount(Key0, batch, out var account, RootLevel);
        Assert.AreEqual(Nonce1, account.Nonce);
        Assert.AreEqual(Balance1, account.Balance);
    }

    [Test]
    public void Works_with_bucket_collision()
    {
        var page = AllocPage();
        page.Clear();

        var batch = NewBatch(BatchId);

        var dataPage = new DataPage(page);
        var ctx1 = new SetContext(Key1a, Balance0, Nonce0, batch);
        var ctx2 = new SetContext(Key1b, Balance1, Nonce1, batch);

        var updated = dataPage.Set(ctx1, RootLevel);
        updated = new DataPage(updated).Set(ctx2, RootLevel);

        new DataPage(updated).GetAccount(Key1a, batch, out var account, RootLevel);
        Assert.AreEqual(Nonce0, account.Nonce);
        Assert.AreEqual(Balance0, account.Balance);

        new DataPage(updated).GetAccount(Key1b, batch, out account, RootLevel);
        Assert.AreEqual(Nonce1, account.Nonce);
        Assert.AreEqual(Balance1, account.Balance);
    }

    [Test]
    public void Page_overflows()
    {
        var page = AllocPage();
        page.Clear();

        var batch = NewBatch(BatchId);
        var dataPage = new DataPage(page);

        const int count = 1 * 1024 * 1024;

        for (uint i = 0; i < count; i++)
        {
            var key = Key1a;
            BinaryPrimitives.WriteUInt32LittleEndian(key.BytesAsSpan, i);

            var ctx = new SetContext(key, i, i, batch);
            dataPage = new DataPage(dataPage.Set(ctx, RootLevel));
        }

        for (uint i = 0; i < count; i++)
        {
            var key = Key1a;
            BinaryPrimitives.WriteUInt32LittleEndian(key.BytesAsSpan, i);

            dataPage.GetAccount(key, batch, out var account, RootLevel);
            account.Should().Be(new Account(i, i));
        }
    }

    [Test(Description = "The scenario to test handling updates over multiple batches so that the pages are properly linked and used.")]
    public void Multiple_batches()
    {
        var page = AllocPage();
        page.Clear();

        var batch = NewBatch(BatchId);
        var dataPage = new DataPage(page);

        const int count = 32 * 1024;
        const int batchEvery = 32;

        for (uint i = 0; i < count; i++)
        {
            var key = Key1a;
            BinaryPrimitives.WriteUInt32LittleEndian(key.BytesAsSpan, i);

            if (i % batchEvery == 0)
            {
                batch = batch.Next();
            }

            var ctx = new SetContext(key, i, i, batch);

            dataPage = new DataPage(dataPage.Set(ctx, RootLevel));
        }

        for (uint i = 0; i < count; i++)
        {
            var key = Key1a;
            BinaryPrimitives.WriteUInt32LittleEndian(key.BytesAsSpan, i);

            dataPage.GetAccount(key, batch, out var account, RootLevel);
            account.Should().Be(new Account(i, i));
        }
    }
}