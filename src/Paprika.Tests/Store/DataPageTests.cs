using System.Buffers.Binary;
using System.Runtime.InteropServices;
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

        dataPage
            .Set(Key0, value, batch)
            .GetAssert(Key0, value, batch);
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

        dataPage
            .Set(Key0, value0, batch)
            .Set(Key0, value1, batch)
            .GetAssert(Key0, value1, batch);
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

        dataPage
            .Set(Key1A, value1A, batch)
            .Set(Key1B, value1B, batch)
            .GetAssert(Key1A, value1A, batch)
            .GetAssert(Key1B, value1B, batch);
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
            dataPage = dataPage.Set(random.NextKeccak(), GetValue(i), batch);
        }

        random = new Random(seed);
        for (var i = 0; i < count; i++)
        {
            dataPage.GetAssert(random.NextKeccak(), GetValue(i), batch, i);
        }
    }
    
    [Test]
    public void Page_overflows_big_shared_distro()
    {
        var page = AllocPage();
        page.Clear();

        var batch = NewBatch(BatchId);
        
        var dataPage = new DataPage(page);
        const int count = 128; // little endian, should have 128 / 16 = 8 items per nibble
        for (var i = 0; i < count; i++)
        {
            dataPage = dataPage.Set(Generate(i), BigValue(i), batch);
            
            for (var j = 0; j <= i; j++)
            {
                dataPage.GetAssert(Generate(j), BigValue(j), batch, i);
            }
        }

        return;

        static Keccak Generate(int i)
        {
            Keccak k = default;
            k.BytesAsSpan.Fill(0xFF);
            BinaryPrimitives.WriteInt32LittleEndian(k.BytesAsSpan, i);
            return k;
        }
        
        static byte[] BigValue(int i)
        {
            var bytes = new byte[1024];
            BinaryPrimitives.WriteInt32LittleEndian(bytes, i);
            return bytes;
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

            dataPage = dataPage.Set(key, GetValue(i), batch);
        }

        for (int i = 0; i < count; i++)
        {
            var key = GetKey(i);

            dataPage.ShouldHave(key, GetValue(i), batch, i);
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
        var emptyPath = NibblePath.Empty;
        dataPage = dataPage.Set(NibblePath.Empty, data, batch).Cast<DataPage>();

        for (var i = 0; i < count; i++)
        {
            var key = GetKey(i);
            dataPage = dataPage.Set(key, GetValue(i), batch);
        }

        // assert
        dataPage.GetAssert(emptyPath, data, batch);

        for (int i = 0; i < count; i++)
        {
            dataPage.GetAssert(GetKey(i), GetValue(i), batch);
        }
    }

    // [TestCase(1, 1000, TestName = "Value at the beginning")]
    // [TestCase(999, 1000, TestName = "Value at the end")]
    // public void Delete(int deleteAt, int count)
    // {
    //     var page = AllocPage();
    //     page.Clear();
    //
    //     var batch = NewBatch(BatchId);
    //     var dataPage = new DataPage(page);
    //
    //     var account = NibblePath.FromKey(GetKey(0));
    //
    //     const int seed = 13;
    //     var random = new Random(seed);
    //
    //     for (var i = 0; i < count; i++)
    //     {
    //         var storagePath = NibblePath.FromKey(random.NextKeccak());
    //         var merkleKey = Key.Raw(account, DataType.Merkle, storagePath);
    //         var value = GetValue(i);
    //
    //         dataPage = dataPage.Set(merkleKey, value, batch).Cast<DataPage>();
    //     }
    //
    //     // delete
    //     random = new Random(seed);
    //     for (var i = 0; i < deleteAt; i++)
    //     {
    //         // skip till set
    //         random.NextKeccak();
    //     }
    //
    //     {
    //         var storagePath = NibblePath.FromKey(random.NextKeccak());
    //         var merkleKey = Key.Raw(account, DataType.Merkle, storagePath);
    //         dataPage = dataPage.Set(new SetContext(merkleKey, ReadOnlySpan<byte>.Empty, batch)).Cast<DataPage>();
    //     }
    //
    //     // assert
    //     random = new Random(seed);
    //
    //     for (var i = 0; i < count; i++)
    //     {
    //         var storagePath = NibblePath.FromKey(random.NextKeccak());
    //         var merkleKey = Key.Raw(account, DataType.Merkle, storagePath);
    //         dataPage.TryGet(merkleKey, batch, out var actual).Should().BeTrue();
    //         var value = i == deleteAt ? ReadOnlySpan<byte>.Empty : GetValue(i);
    //         actual.SequenceEqual(value).Should().BeTrue($"Does not match for i: {i} and delete at: {deleteAt}");
    //     }
    // }

    // [Test]
    // [Ignore("This test should be removed or rewritten")]
    // public void Small_prefix_tree_with_regular()
    // {
    //     var page = AllocPage();
    //     page.Clear();
    //
    //     var batch = NewBatch(BatchId);
    //     var dataPage = new DataPage(page);
    //
    //     const int count = 19; // this is the number until a prefix tree is extracted
    //
    //     var account = Keccak.EmptyTreeHash;
    //
    //     dataPage = dataPage
    //         .Set(account, GetValue(0), batch)
    //         .Set(account, GetValue(1), batch);
    //
    //     for (var i = 0; i < count; i++)
    //     {
    //         var storage = GetKey(i);
    //
    //         dataPage = dataPage
    //             .SetMerkle(account, NibblePath.FromKey(storage), GetValue(i), batch);
    //     }
    //
    //     // write 256 more to fill up the page for each nibble
    //     for (var i = 0; i < ushort.MaxValue; i++)
    //     {
    //         dataPage = dataPage.SetAccount(GetKey(i), GetValue(i), batch);
    //     }
    //
    //     // assert
    //     dataPage.ShouldHaveAccount(account, GetValue(0), batch);
    //     dataPage.ShouldHaveMerkle(account, GetValue(1), batch);
    //
    //     for (var i = 0; i < count; i++)
    //     {
    //         var storage = GetKey(i);
    //
    //         dataPage.ShouldHaveStorage(account, storage, GetValue(i), batch);
    //         dataPage.ShouldHaveMerkle(account, NibblePath.FromKey(storage), GetValue(i), batch);
    //     }
    //
    //     // write 256 more to fill up the page for each nibble
    //     for (var i = 0; i < ushort.MaxValue; i++)
    //     {
    //         dataPage.ShouldHaveAccount(GetKey(i), GetValue(i), batch);
    //     }
    // }

    // [Test]
    // public void Massive_prefix_tree()
    // {
    //     var page = AllocPage();
    //     page.Clear();
    //
    //     var batch = NewBatch(BatchId);
    //     var dataPage = new DataPage(page);
    //
    //     const int count = 10_000;
    //
    //     var account = Keccak.EmptyTreeHash;
    //
    //     dataPage = dataPage
    //         .Set(account, GetValue(0), batch)
    //
    //     for (var i = 0; i < count; i++)
    //     {
    //         var storage = GetKey(i);
    //         dataPage = dataPage
    //             .Set(account, GetValue(i), batch)
    //             .SetMerkle(account, GetMerkleKey(storage, i), GetValue(i), batch);
    //     }
    //
    //     // assert
    //     dataPage.ShouldHaveAccount(account, GetValue(0), batch);
    //     dataPage.ShouldHaveMerkle(account, GetValue(1), batch);
    //
    //     for (var i = 0; i < count; i++)
    //     {
    //         var storage = GetKey(i);
    //
    //         dataPage.ShouldHaveStorage(account, storage, GetValue(i), batch);
    //         dataPage.ShouldHaveMerkle(account, GetMerkleKey(storage, i), GetValue(i), batch);
    //     }
    //
    //     return;
    //
    //     static NibblePath GetMerkleKey(in Keccak storage, int i)
    //     {
    //         return NibblePath.FromKey(storage).SliceTo(Math.Min(i + 1, NibblePath.KeccakNibbleCount));
    //     }
    // }

    // [Test]
    // public void Different_at_start_keys()
    // {
    //     var page = AllocPage();
    //     page.Clear();
    //
    //     var batch = NewBatch(BatchId);
    //     var dataPage = new DataPage(page);
    //
    //     const int count = 10_000;
    //
    //     Span<byte> dest = stackalloc byte[sizeof(int)];
    //     Span<byte> store = stackalloc byte[StoreKey.StorageKeySize];
    //
    //     const DataType compressedAccount = DataType.Account | DataType.CompressedAccount;
    //     const DataType compressedMerkle = DataType.Merkle | DataType.CompressedAccount;
    //
    //     ReadOnlySpan<byte> accountValue = stackalloc byte[1] { (byte)compressedAccount };
    //     ReadOnlySpan<byte> merkleValue = stackalloc byte[1] { (byte)compressedMerkle };
    //
    //     for (var i = 0; i < count; i++)
    //     {
    //         BinaryPrimitives.WriteInt32LittleEndian(dest, i);
    //         var path = NibblePath.FromKey(dest);
    //
    //         // account
    //         {
    //             var accountKey = Key.Raw(path, compressedAccount, NibblePath.Empty);
    //             var accountStoreKey = StoreKey.Encode(accountKey, store);
    //
    //             dataPage = new DataPage(dataPage.Set(NibblePath.FromKey(accountStoreKey.Payload), accountValue, batch));
    //         }
    //
    //         // merkle
    //         {
    //             var merkleKey = Key.Raw(path, compressedMerkle, NibblePath.Empty);
    //             var merkleStoreKey = StoreKey.Encode(merkleKey, store);
    //
    //             dataPage = new DataPage(dataPage.Set(NibblePath.FromKey(merkleStoreKey.Payload), merkleValue, batch));
    //         }
    //     }
    //
    //     for (var i = 0; i < count; i++)
    //     {
    //         BinaryPrimitives.WriteInt32LittleEndian(dest, i);
    //         var path = NibblePath.FromKey(dest);
    //
    //         // account
    //         {
    //             var accountKey = Key.Raw(path, compressedAccount, NibblePath.Empty);
    //             var accountStoreKey = StoreKey.Encode(accountKey, store);
    //
    //             dataPage.TryGet(NibblePath.FromKey(accountStoreKey.Payload), batch, out var value).Should().BeTrue();
    //             value.SequenceEqual(accountValue).Should().BeTrue();
    //         }
    //
    //         // merkle
    //         {
    //             var merkleKey = Key.Raw(path, compressedMerkle, NibblePath.Empty);
    //             var merkleStoreKey = StoreKey.Encode(merkleKey, store);
    //
    //             dataPage.TryGet(NibblePath.FromKey(merkleStoreKey.Payload), batch, out var value).Should().BeTrue();
    //             value.SequenceEqual(merkleValue).Should().BeTrue();
    //         }
    //     }
    // }
}