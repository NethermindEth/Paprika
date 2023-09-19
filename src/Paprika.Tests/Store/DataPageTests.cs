using System.Buffers.Binary;
using System.Diagnostics;
using FluentAssertions;
using Nethermind.Int256;
using NUnit.Framework;
using Paprika.Crypto;
using Paprika.Data;
using Paprika.Store;
using static Paprika.Tests.Values;

namespace Paprika.Tests.Store;

public class DataPageTests : BasePageTests
{
    private const uint BatchId = 1;

    private static byte[] GetValue(int i) => new UInt256((uint)i).ToBigEndian();

    private static Keccak GetKey(int i)
    {
        var keccak = Keccak.Zero;
        BinaryPrimitives.WriteInt32LittleEndian(keccak.BytesAsSpan, i);
        return keccak;
    }

    [Test]
    public void Set_then_Get()
    {
        var page = AllocPage();
        page.Clear();

        var batch = NewBatch(BatchId);
        var dataPage = new DataPage(page);

        var value = GetValue(0);

        var updated = dataPage.SetAccount(Key0, value, batch);
        updated.ShouldHaveAccount(Key0, value, batch);
    }

    [Test]
    public void Update_key()
    {
        var page = AllocPage();
        page.Clear();

        var batch = NewBatch(BatchId);
        var value0 = GetValue(0);
        var value1 = GetValue(1);

        var dataPage = new DataPage(page);

        var updated = dataPage
            .SetAccount(Key0, value0, batch)
            .SetAccount(Key0, value1, batch);

        updated.ShouldHaveAccount(Key0, value1, batch);
    }

    [Test]
    public void Works_with_bucket_collision()
    {
        var page = AllocPage();
        page.Clear();

        var batch = NewBatch(BatchId);

        var dataPage = new DataPage(page);
        var value1A = GetValue(0);
        var value1B = GetValue(1);

        var updated = dataPage
            .SetAccount(Key1A, value1A, batch)
            .SetAccount(Key1B, value1B, batch);

        updated.ShouldHaveAccount(Key1A, value1A, batch);
        updated.ShouldHaveAccount(Key1B, value1B, batch);
    }

    [Test]
    public void Page_overflows()
    {
        var page = AllocPage();
        page.Clear();

        var batch = NewBatch(BatchId);
        var dataPage = new DataPage(page);

        const int count = 128 * 1024;
        const int seed = 13;

        var random = new Random(seed);
        for (var i = 0; i < count; i++)
        {
            dataPage = dataPage.SetAccount(random.NextKeccak(), GetValue(i), batch);
        }

        random = new Random(seed);
        for (var i = 0; i < count; i++)
        {
            dataPage.ShouldHaveAccount(random.NextKeccak(), GetValue(i), batch, i);
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

        for (int i = 0; i < count; i++)
        {
            var key = GetKey(i);
            var address = key;
            var value = GetValue(i);

            dataPage = dataPage
                .SetAccount(key, value, batch)
                .SetStorage(key, address, value, batch);
        }

        for (int i = 0; i < count; i++)
        {
            var key = GetKey(i);
            var address = key;
            var value = GetValue(i);

            dataPage.ShouldHaveAccount(key, value, batch);
            dataPage.ShouldHaveStorage(key, address, value, batch);
        }
    }

    [Test(Description =
        "The scenario to test handling updates over multiple batches so that the pages are properly linked and used.")]
    public void Multiple_batches()
    {
        var page = AllocPage();
        page.Clear();

        var batch = NewBatch(BatchId);
        var dataPage = new DataPage(page);

        const int count = 32 * 1024;
        const int batchEvery = 32;

        for (int i = 0; i < count; i++)
        {
            var key = GetKey(i);

            if (i % batchEvery == 0)
            {
                batch = batch.Next();
            }

            dataPage = dataPage.SetAccount(key, GetValue(i), batch);
        }

        for (int i = 0; i < count; i++)
        {
            var key = GetKey(i);

            dataPage.ShouldHaveAccount(key, GetValue(i), batch);
        }
    }

    [Test(Description = "Ensures that tree can hold entries with NibblePaths of various lengths")]
    public void Var_length_NibblePaths()
    {
        Span<byte> data = stackalloc byte[1] { 13 };
        var page = AllocPage();
        page.Clear();

        var batch = NewBatch(BatchId);
        var dataPage = new DataPage(page);

        // big enough to fill the page
        const int count = 200;

        // set the empty path which may happen on var-length scenarios
        var keccakKey = Key.Account(NibblePath.Empty);
        dataPage = dataPage.Set(new SetContext(keccakKey, data, batch)).Cast<DataPage>();

        for (var i = 0; i < count; i++)
        {
            var key = GetKey(i);
            dataPage = dataPage.SetAccount(key, GetValue(i), batch);
        }

        // assert
        dataPage.TryGet(keccakKey, batch, out var value).Should().BeTrue();
        value.SequenceEqual(data).Should().BeTrue();

        for (int i = 0; i < count; i++)
        {
            var key = GetKey(i);
            var path = NibblePath.FromKey(key);
            dataPage.ShouldHaveAccount(key, GetValue(i), batch);
        }
    }

    [Test]
    public void Page_overflows_with_merkle()
    {
        var page = AllocPage();
        page.Clear();

        var batch = NewBatch(BatchId);
        var dataPage = new DataPage(page);

        const int seed = 13;
        var rand = new Random(seed);

        const int count = 10_000;
        for (int i = 0; i < count; i++)
        {
            var account = NibblePath.FromKey(rand.NextKeccak());

            var accountKey = Key.Raw(account, DataType.Account, NibblePath.Empty);
            var merkleKey = Key.Raw(account, DataType.Merkle, NibblePath.Empty);

            dataPage = dataPage.Set(new SetContext(accountKey, GetAccountValue(i), batch)).Cast<DataPage>();
            dataPage = dataPage.Set(new SetContext(merkleKey, GetMerkleValue(i), batch)).Cast<DataPage>();
        }

        rand = new Random(seed);

        for (int i = 0; i < count; i++)
        {
            var account = NibblePath.FromKey(rand.NextKeccak());

            var accountKey = Key.Raw(account, DataType.Account, NibblePath.Empty);
            var merkleKey = Key.Raw(account, DataType.Merkle, NibblePath.Empty);

            dataPage.TryGet(accountKey, batch, out var actualAccountValue).Should().BeTrue();
            actualAccountValue.SequenceEqual(GetAccountValue(i)).Should().BeTrue();

            dataPage.TryGet(merkleKey, batch, out var actualMerkleValue).Should().BeTrue();
            actualMerkleValue.SequenceEqual(GetMerkleValue(i)).Should().BeTrue();
        }

        static byte[] GetAccountValue(int i) => BitConverter.GetBytes(i * 2 + 1);
        static byte[] GetMerkleValue(int i) => BitConverter.GetBytes(i * 2);
    }

    [TestCase(1, 1000, TestName = "Value at the beginning")]
    [TestCase(999, 1000, TestName = "Value at the end")]
    public void Delete(int deleteAt, int count)
    {
        var page = AllocPage();
        page.Clear();

        var batch = NewBatch(BatchId);
        var dataPage = new DataPage(page);

        var account = NibblePath.FromKey(GetKey(0));

        const int seed = 13;
        var random = new Random(seed);

        for (var i = 0; i < count; i++)
        {
            var storagePath = NibblePath.FromKey(random.NextKeccak());
            var merkleKey = Key.Raw(account, DataType.Merkle, storagePath);
            var value = GetValue(i);

            dataPage = dataPage.Set(new SetContext(merkleKey, value, batch)).Cast<DataPage>();
        }

        // delete
        random = new Random(seed);
        for (var i = 0; i < deleteAt; i++)
        {
            // skip till set
            random.NextKeccak();
        }

        {
            var storagePath = NibblePath.FromKey(random.NextKeccak());
            var merkleKey = Key.Raw(account, DataType.Merkle, storagePath);
            dataPage = dataPage.Set(new SetContext(merkleKey, ReadOnlySpan<byte>.Empty, batch)).Cast<DataPage>();
        }

        // assert
        random = new Random(seed);

        for (var i = 0; i < count; i++)
        {
            var storagePath = NibblePath.FromKey(random.NextKeccak());
            var merkleKey = Key.Raw(account, DataType.Merkle, storagePath);
            dataPage.TryGet(merkleKey, batch, out var actual).Should().BeTrue();
            var value = i == deleteAt ? ReadOnlySpan<byte>.Empty : GetValue(i);
            actual.SequenceEqual(value).Should().BeTrue($"Does not match for i: {i} and delete at: {deleteAt}");
        }
    }

    [Test]
    public void PrefixTreeExtraction()
    {
        var page = AllocPage();
        page.Clear();

        var batch = NewBatch(BatchId);
        var dataPage = new DataPage(page);

        const int count = 19; // this is the number until a prefix tree is extracted

        var account = Keccak.EmptyTreeHash;

        dataPage = dataPage
            .SetAccount(account, GetValue(0), batch)
            .SetMerkle(account, GetValue(1), batch);

        for (var i = 0; i < count; i++)
        {
            var storage = GetKey(i);

            dataPage = dataPage
                .SetStorage(account, storage, GetValue(i), batch)
                .SetMerkle(account, NibblePath.FromKey(storage), GetValue(i), batch);
        }

        // write 256 more to fill up the page for each nibble
        for (var i = 0; i < ushort.MaxValue; i++)
        {
            dataPage = dataPage.SetAccount(GetKey(i), GetValue(i), batch);
        }

        // assert
        dataPage.ShouldHaveAccount(account, GetValue(0), batch);
        dataPage.ShouldHaveMerkle(account, GetValue(1), batch);

        for (var i = 0; i < count; i++)
        {
            var storage = GetKey(i);

            dataPage.ShouldHaveStorage(account, storage, GetValue(i), batch);
            dataPage.ShouldHaveMerkle(account, NibblePath.FromKey(storage), GetValue(i), batch);
        }

        // write 256 more to fill up the page for each nibble
        for (var i = 0; i < ushort.MaxValue; i++)
        {
            dataPage.ShouldHaveAccount(GetKey(i), GetValue(i), batch);
        }
    }
}