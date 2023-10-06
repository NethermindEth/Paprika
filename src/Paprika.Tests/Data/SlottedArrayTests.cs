using FluentAssertions;
using NUnit.Framework;
using Paprika.Crypto;
using Paprika.Data;
using Paprika.Store;

namespace Paprika.Tests.Data;

public class SlottedArrayTests
{
    private static NibblePath Key0 => NibblePath.FromKey(Values.Key0);
    private static ReadOnlySpan<byte> Data0 => new byte[] { 23 };

    private static NibblePath Key1 => NibblePath.FromKey(Values.Key1);
    private static ReadOnlySpan<byte> Data1 => new byte[] { 29, 31 };

    private static NibblePath Key2 => NibblePath.FromKey(Values.Key2);
    private static ReadOnlySpan<byte> Data2 => new byte[] { 37, 39 };

    private static ReadOnlySpan<byte> Data3 => new byte[] { 39, 41, 43 };

    private static readonly Keccak StorageCell0 = Values.Key3;
    private static readonly Keccak StorageCell1 = Values.Key4;
    private static readonly Keccak StorageCell2 = Values.Key5;

    [Test]
    public void Set_Get_Delete_Get_AnotherSet()
    {
        Span<byte> span = stackalloc byte[48];
        var map = new SlottedArray(span);

        map.SetAssert(Key.Account(Key0), Data0);

        map.GetAssert(Key.Account(Key0), Data0);

        map.DeleteAssert(Key.Account(Key0));
        map.GetShouldFail(Key.Account(Key0));

        // should be ready to accept some data again

        map.SetAssert(Key.Account(Key1), Data1, "Should have memory after previous delete");

        map.GetAssert(Key.Account(Key1), Data1);
    }

    [Test]
    public void Enumerate_all()
    {
        Span<byte> span = stackalloc byte[256];
        var map = new SlottedArray(span);

        var key0 = Span<byte>.Empty;
        Span<byte> key1 = stackalloc byte[1] { 7 };
        Span<byte> key2 = stackalloc byte[2] { 7, 13 };

        map.SetAssert(key0, Data0);
        map.SetAssert(key1, Data1);
        map.SetAssert(key2, Data2);

        using var e = map.EnumerateAll();

        e.MoveNext().Should().BeTrue();
        e.Current.Key.SequenceEqual(key0).Should().BeTrue();
        e.Current.RawData.SequenceEqual(Data0).Should().BeTrue();

        e.MoveNext().Should().BeTrue();
        e.Current.Key.SequenceEqual(key1).Should().BeTrue();
        e.Current.RawData.SequenceEqual(Data1).Should().BeTrue();

        e.MoveNext().Should().BeTrue();
        e.Current.Key.SequenceEqual(key2).Should().BeTrue();
        e.Current.RawData.SequenceEqual(Data2).Should().BeTrue();

        e.MoveNext().Should().BeFalse();
    }

    [Test]
    public void Defragment_when_no_more_space()
    {
        // by trial and error, found the smallest value that will allow to put these two
        Span<byte> span = stackalloc byte[88];
        var map = new SlottedArray(span);

        map.SetAssert(Key.Account(Key0), Data0);
        map.SetAssert(Key.Account(Key1), Data1);

        map.DeleteAssert(Key.Account(Key0));

        map.SetAssert(Key.Account(Key2), Data2, "Should retrieve space by running internally the defragmentation");

        // should contains no key0, key1 and key2 now
        map.GetShouldFail(Key.Account(Key0));

        map.GetAssert(Key.Account(Key1), Data1);
        map.GetAssert(Key.Account(Key2), Data2);
    }

    [Test]
    public void Update_in_situ()
    {
        // by trial and error, found the smallest value that will allow to put these two
        Span<byte> span = stackalloc byte[48];
        var map = new SlottedArray(span);

        map.SetAssert(Key.Account(Key1), Data1);
        map.SetAssert(Key.Account(Key1), Data2);

        map.GetAssert(Key.Account(Key1), Data2);
    }

    [Test]
    public void Update_in_resize()
    {
        // by trial and error, found the smallest value that will allow to put these two
        Span<byte> span = stackalloc byte[56];
        var map = new SlottedArray(span);

        map.SetAssert(Key.Account(Key0), Data0);
        map.SetAssert(Key.Account(Key0), Data2);

        map.GetAssert(Key.Account(Key0), Data2);
    }

    [Test]
    public void Account_and_multiple_storage_cells()
    {
        Span<byte> span = stackalloc byte[512];
        var map = new SlottedArray(span);

        map.SetAssert(Key.Account(Key0), Data0);
        map.SetAssert(Key.StorageCell(Key0, StorageCell0), Data1);
        map.SetAssert(Key.StorageCell(Key0, StorageCell1), Data2);
        map.SetAssert(Key.StorageCell(Key0, StorageCell2), Data3);

        map.GetAssert(Key.Account(Key0), Data0);
        map.GetAssert(Key.StorageCell(Key0, StorageCell0), Data1);
        map.GetAssert(Key.StorageCell(Key0, StorageCell1), Data2);
        map.GetAssert(Key.StorageCell(Key0, StorageCell2), Data3);
    }

    [Test]
    public void Different_accounts_same_cells()
    {
        Span<byte> span = stackalloc byte[512];
        var map = new SlottedArray(span);

        map.SetAssert(Key.StorageCell(Key0, StorageCell0), Data1);
        map.SetAssert(Key.StorageCell(Key1, StorageCell0), Data2);

        map.GetAssert(Key.StorageCell(Key0, StorageCell0), Data1);
        map.GetAssert(Key.StorageCell(Key1, StorageCell0), Data2);
    }
}

file static class FixedMapTestExtensions
{
    public static void SetAssert(this SlottedArray map, in Key key, ReadOnlySpan<byte> data, string? because = null)
    {
        var storeKey = StoreKey.Encode(key, stackalloc byte[StoreKey.GetMaxByteSize(key)]);
        map.TrySet(storeKey.Payload, data).Should().BeTrue(because ?? "TrySet should succeed");
    }

    public static void SetAssert(this SlottedArray map, in ReadOnlySpan<byte> key, ReadOnlySpan<byte> data, string? because = null)
    {
        map.TrySet(key, data).Should().BeTrue(because ?? "TrySet should succeed");
    }

    public static void DeleteAssert(this SlottedArray map, in Key key)
    {
        var storeKey = StoreKey.Encode(key, stackalloc byte[StoreKey.GetMaxByteSize(key)]);
        map.Delete(storeKey.Payload).Should().BeTrue("Delete should succeed");
    }

    public static void GetAssert(this SlottedArray map, in Key key, ReadOnlySpan<byte> expected)
    {
        var storeKey = StoreKey.Encode(key, stackalloc byte[StoreKey.GetMaxByteSize(key)]);
        map.TryGet(storeKey.Payload, out var actual).Should().BeTrue();
        actual.SequenceEqual(expected).Should().BeTrue("Actual data should equal expected");
    }

    public static void GetShouldFail(this SlottedArray map, in Key key)
    {
        var storeKey = StoreKey.Encode(key, stackalloc byte[StoreKey.GetMaxByteSize(key)]);
        map.TryGet(storeKey.Payload, out var actual).Should().BeFalse("The key should not exist");
    }
}