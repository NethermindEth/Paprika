using System.Buffers.Binary;
using FluentAssertions;
using Nethermind.Int256;
using NUnit.Framework;
using Paprika.Crypto;
using Paprika.Data;
using Paprika.Data.Map;
using Paprika.Db;
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

        const int offset = 0x12345678;

        for (uint i = 0; i < count; i++)
        {
            var key = Key1a;
            BinaryPrimitives.WriteUInt32LittleEndian(key.BytesAsSpan, i + offset);

            dataPage = new DataPage(dataPage.SetAccount(NibblePath.FromKey(key), new Account(i, i), batch));
        }

        for (uint i = 0; i < count; i++)
        {
            var key = Key1a;
            BinaryPrimitives.WriteUInt32LittleEndian(key.BytesAsSpan, i + offset);

            var account = dataPage.GetAccount(NibblePath.FromKey(key), batch);
            account.Should().Be(new Account(i, i));
        }
    }

    [Test(Description = "The test for a page that has some accounts and their storages with 50-50 ratio")]
    public void Page_overflows_with_some_storage_and_some_accounts()
    {
        var page = AllocPage();
        page.Clear();

        var batch = NewBatch(BatchId);
        var dataPage = new DataPage(page);

        const int count = 35;

        for (uint i = 0; i < count; i++)
        {
            var key = GetKey(i);
            var address = GetStorageAddress(i);
            var path = NibblePath.FromKey(key);

            dataPage = dataPage
                .SetAccount(path, GetAccount(i), batch)
                .Cast<DataPage>()
                .SetStorage(path, address, GetStorageValue(i), batch)
                .Cast<DataPage>();
        }

        for (uint i = 0; i < count; i++)
        {
            var key = GetKey(i);
            var address = GetStorageAddress(i);
            var path = NibblePath.FromKey(key);

            dataPage.GetAccount(path, batch).Should().Be(GetAccount(i));
            dataPage.GetStorage(path, address, batch).Should().Be(GetStorageValue(i));
        }

        static Keccak GetStorageAddress(uint i)
        {
            var address = Key1a;
            BinaryPrimitives.WriteUInt32LittleEndian(address.BytesAsSpan, i);
            return address;
        }

        Keccak GetKey(uint i)
        {
            Keccak key = default;
            BinaryPrimitives.WriteUInt32LittleEndian(key.BytesAsSpan, i);
            return key;
        }

        Account GetAccount(uint i) => new(i, i);

        UInt256 GetStorageValue(uint i) => i;
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

    [Test(Description = "Ensures that tree can hold entries with NibblePaths of various lengths")]
    public void Var_length_NibblePaths()
    {
        var page = AllocPage();
        page.Clear();

        var batch = NewBatch(BatchId);
        var dataPage = new DataPage(page);

        // big enough to fill the page
        const int count = 200;

        // set the empty path which may happen on var-length scenarios
        var keccakKey = Key.KeccakOrRlp(NibblePath.Empty);
        dataPage = dataPage.Set(new SetContext(keccakKey, Span<byte>.Empty, batch)).Cast<DataPage>();

        for (uint i = 0; i < count; i++)
        {
            var key = GetKey(i);
            var path = NibblePath.FromKey(key);

            dataPage = dataPage
                .SetAccount(path, GetAccount(i), batch)
                .Cast<DataPage>();
        }

        // assert
        dataPage.TryGet(keccakKey, batch, out var value).Should().BeTrue();
        value.Length.Should().Be(0);

        for (uint i = 0; i < count; i++)
        {
            var key = GetKey(i);
            var path = NibblePath.FromKey(key);

            dataPage.GetAccount(path, batch).Should().Be(GetAccount(i));
        }

        Keccak GetKey(uint i)
        {
            Keccak key = default;
            // big endian so that zeroes go first
            BinaryPrimitives.WriteUInt32BigEndian(key.BytesAsSpan, i);
            return key;
        }

        Account GetAccount(uint i) => new(i, i);
    }
}