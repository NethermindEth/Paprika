using FluentAssertions;
using NUnit.Framework;
using Paprika.Crypto;
using Paprika.Data;

namespace Paprika.Tests.Data;

public class NibbleBasedMapTests
{
    private static NibblePath Key0 => NibblePath.FromKey(new byte[] { 0x12, 0x34, 0x56, 0x78, 0x90 });
    private static ReadOnlySpan<byte> Data0 => new byte[] { 23 };

    private static NibblePath Key1 => NibblePath.FromKey(new byte[] { 0x12, 0x34, 0x56, 0x78, 0x99 });
    private static ReadOnlySpan<byte> Data1 => new byte[] { 29, 31 };

    private static NibblePath Key2 => NibblePath.FromKey(new byte[] { 19, 21, 23, 29, 23 });
    private static ReadOnlySpan<byte> Data2 => new byte[] { 37, 39 };

    private static ReadOnlySpan<byte> Data3 => new byte[] { 39, 41, 43 };

    private static readonly Keccak StorageCell0 = Keccak.Compute(new byte[] { 2, 43, 4, 5, 34 });
    private static readonly Keccak StorageCell1 = Keccak.Compute(new byte[] { 2, 43, 4, });
    private static readonly Keccak StorageCell2 = Keccak.Compute(new byte[] { 2, 43, });

    [Test]
    public void Set_Get_Delete_Get_AnotherSet()
    {
        Span<byte> span = stackalloc byte[NibbleBasedMap.MinSize];
        var map = new NibbleBasedMap(span);

        map.SetAssert(Key.Account(Key0), Data0);

        map.GetAssert(Key.Account(Key0), Data0);

        map.Delete(Key.Account(Key0)).Should().BeTrue("Should find and delete entry");
        map.TryGet(Key.Account(Key0), out _).Should().BeFalse("The entry shall no longer exist");

        // should be ready to accept some data again

        map.TrySet(Key.Account(Key1), Data1).Should().BeTrue("Should have memory after previous delete");

        map.GetAssert(Key.Account(Key1), Data1);
    }

    [TestCase(0, 10)]
    [TestCase(0, 9)]
    [TestCase(1, 9)]
    [TestCase(1, 8)]
    public void Enumerate_nibble(int from, int length)
    {
        Span<byte> span = stackalloc byte[256];
        var map = new NibbleBasedMap(span);

        var path0 = Key0.SliceFrom(from).SliceTo(length);
        var path1 = Key1.SliceFrom(from).SliceTo(length);

        var key0 = Key.Account(path0);
        var key1 = Key.Account(path1);

        map.SetAssert(key0, Data0);
        map.SetAssert(key1, Data1);

        Console.WriteLine($"Expected keys: {key0.Path.ToString()} and {key1.Path.ToString()}");
        Console.WriteLine("Actual: ");

        using var e = map.EnumerateNibble(key0.Path.FirstNibble);

        e.MoveNext().Should().BeTrue();
        e.Current.Key.Path.ToString().Should().Be(path0.ToString());
        e.Current.RawData.SequenceEqual(Data0).Should().BeTrue();

        e.MoveNext().Should().BeTrue();
        e.Current.Key.Path.ToString().Should().Be(path1.ToString());
        e.Current.RawData.SequenceEqual(Data1).Should().BeTrue();

        e.MoveNext().Should().BeFalse();
    }

    [Test]
    public void Enumerate_all()
    {
        Span<byte> span = stackalloc byte[256];
        var map = new NibbleBasedMap(span);

        var key0 = Key.Account(Key0);
        var key1 = Key.Account(Key2);
        var key2 = Key.Account(NibblePath.Empty);

        map.SetAssert(key0, Data0);
        map.SetAssert(key1, Data1);
        map.SetAssert(key2, Data2);

        using var e = map.EnumerateAll();

        e.MoveNext().Should().BeTrue();
        e.Current.Key.Path.ToString().Should().Be(Key0.ToString());
        e.Current.RawData.SequenceEqual(Data0).Should().BeTrue();

        e.MoveNext().Should().BeTrue();
        e.Current.Key.Path.ToString().Should().Be(Key2.ToString());
        e.Current.RawData.SequenceEqual(Data1).Should().BeTrue();

        e.MoveNext().Should().BeTrue();
        e.Current.Key.Path.ToString().Should().Be(NibblePath.Empty.ToString());
        e.Current.RawData.SequenceEqual(Data2).Should().BeTrue();

        e.MoveNext().Should().BeFalse();
    }

    [Test]
    public void Defragment_when_no_more_space()
    {
        // by trial and error, found the smallest value that will allow to put these two
        Span<byte> span = stackalloc byte[40];
        var map = new NibbleBasedMap(span);

        map.TrySet(Key.Account(Key0), Data0).Should().BeTrue();
        map.TrySet(Key.Account(Key1), Data1).Should().BeTrue();

        map.Delete(Key.Account(Key0)).Should().BeTrue();

        map.TrySet(Key.Account(Key2), Data2).Should()
            .BeTrue("Should retrieve space by running internally the defragmentation");

        // should contains no key0, key1 and key2 now
        map.TryGet(Key.Account(Key0), out var retrieved).Should().BeFalse();

        map.TryGet(Key.Account(Key1), out retrieved).Should().BeTrue();
        Data1.SequenceEqual(retrieved).Should().BeTrue();

        map.TryGet(Key.Account(Key2), out retrieved).Should().BeTrue();
        Data2.SequenceEqual(retrieved).Should().BeTrue();
    }

    [Test]
    public void Update_in_situ()
    {
        // by trial and error, found the smallest value that will allow to put these two
        Span<byte> span = stackalloc byte[24];
        var map = new NibbleBasedMap(span);

        map.SetAssert(Key.Account(Key1), Data1);
        map.SetAssert(Key.Account(Key1), Data2);

        map.GetAssert(Key.Account(Key1), Data2);
    }

    [Test]
    public void Update_in_resize()
    {
        // by trial and error, found the smallest value that will allow to put these two
        Span<byte> span = stackalloc byte[24];
        var map = new NibbleBasedMap(span);

        map.SetAssert(Key.Account(Key0), Data0);
        map.SetAssert(Key.Account(Key0), Data2);

        map.GetAssert(Key.Account(Key0), Data2);
    }

    [Test]
    public void Account_and_multiple_storage_cells()
    {
        Span<byte> span = stackalloc byte[512];
        var map = new NibbleBasedMap(span);

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
        var map = new NibbleBasedMap(span);

        map.SetAssert(Key.StorageCell(Key0, StorageCell0), Data1);
        map.SetAssert(Key.StorageCell(Key1, StorageCell0), Data2);

        map.GetAssert(Key.StorageCell(Key0, StorageCell0), Data1);
        map.GetAssert(Key.StorageCell(Key1, StorageCell0), Data2);
    }
}

file static class FixedMapTestExtensions
{
    public static void SetAssert(this NibbleBasedMap map, in Key key, ReadOnlySpan<byte> data)
    {
        map.TrySet(key, data).Should().BeTrue("TrySet should succeed");
    }

    public static void GetAssert(this NibbleBasedMap map, in Key key, ReadOnlySpan<byte> expected)
    {
        map.TryGet(key, out var actual).Should().BeTrue();
        actual.SequenceEqual(expected).Should().BeTrue("Actual data should equal expected");
    }
}