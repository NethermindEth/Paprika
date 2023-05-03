using System.Buffers.Binary;
using FluentAssertions;
using NUnit.Framework;
using Paprika.Crypto;
using Paprika.Pages;
using static Paprika.Tests.Values;

namespace Paprika.Tests;

public class DataPageTests : BasePageTests
{
    const uint BatchId = 1;

    [Test]
    public void Set_then_Get()
    {
        var page = AllocPage();
        page.Clear();

        var batch = NewBatch(BatchId);
        var dataPage = new DataPage(page);
        var path = NibblePath.FromKey(Key0);
        var set = new Account(Balance0, Nonce0);

        var updated = dataPage.SetAccount(path, set, batch);

        var account = new DataPage(updated).GetAccount(path, batch);

        Assert.AreEqual(Nonce0, account.Nonce);
        Assert.AreEqual(Balance0, account.Balance);
    }

    [Test]
    public void Update_key()
    {
        var page = AllocPage();
        page.Clear();

        var batch = NewBatch(BatchId);

        var path0 = NibblePath.FromKey(Key0);

        var dataPage = new DataPage(page);

        var updated = dataPage.SetAccount(path0, new Account(Balance0, Nonce0), batch);
        updated = new DataPage(updated).SetAccount(path0, new Account(Balance1, Nonce1), batch);

        var account = new DataPage(updated).GetAccount(path0, batch);
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
        var path1A = NibblePath.FromKey(Key1a);
        var path1B = NibblePath.FromKey(Key1b);

        var updated = dataPage.SetAccount(path1A, new Account(Balance0, Nonce0), batch);
        updated = new DataPage(updated).SetAccount(path1B, new Account(Balance1, Nonce1), batch);

        var account = new DataPage(updated).GetAccount(path1A, batch);
        Assert.AreEqual(Nonce0, account.Nonce);
        Assert.AreEqual(Balance0, account.Balance);

        account = new DataPage(updated).GetAccount(path1B, batch);
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

            dataPage = new DataPage(dataPage.SetAccount(NibblePath.FromKey(key), new Account(i, i), batch));
        }

        for (uint i = 0; i < count; i++)
        {
            var key = Key1a;
            BinaryPrimitives.WriteUInt32LittleEndian(key.BytesAsSpan, i);

            var account = dataPage.GetAccount(NibblePath.FromKey(key), batch);
            account.Should().Be(new Account(i, i));
        }
    }

    [Test]
    public void Page_overflows_storage()
    {
        var page = AllocPage();
        page.Clear();

        var batch = NewBatch(BatchId);
        var dataPage = new DataPage(page);

        const int count = 56;

        for (uint i = 0; i < count; i++)
        {
            var address = GetStorageAddress(i);

            dataPage = new DataPage(dataPage.SetStorage(NibblePath.FromKey(Key1a), address, i, batch));
        }

        for (uint i = 0; i < count; i++)
        {
            var address = GetStorageAddress(i);

            var value = dataPage.GetStorage(NibblePath.FromKey(Key1a), address, batch);
            value.Should().Be(i);
        }

        static Keccak GetStorageAddress(uint i)
        {
            var address = Key1a;
            BinaryPrimitives.WriteUInt32LittleEndian(address.BytesAsSpan, i);
            return address;
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

            dataPage = new DataPage(dataPage.SetAccount(NibblePath.FromKey(key), new Account(i, i), batch));
        }

        for (uint i = 0; i < count; i++)
        {
            var key = Key1a;
            BinaryPrimitives.WriteUInt32LittleEndian(key.BytesAsSpan, i);

            var account = dataPage.GetAccount(NibblePath.FromKey(key), batch);
            account.Should().Be(new Account(i, i));
        }
    }
}