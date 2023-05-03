using FluentAssertions;
using NUnit.Framework;
using Paprika.Pages;

namespace Paprika.Tests;

public class FixedMapTests
{
    private static NibblePath Key0 => NibblePath.FromKey(new byte[] { 1, 2, 3, 5 });
    private static ReadOnlySpan<byte> Data0 => new byte[] { 23 };
    private static NibblePath Key1 => NibblePath.FromKey(new byte[] { 7, 11, 13, 17 });
    private static ReadOnlySpan<byte> Data1 => new byte[] { 29, 31 };
    private static NibblePath Key2 => NibblePath.FromKey(new byte[] { 19, 21, 23, 29 });
    private static ReadOnlySpan<byte> Data2 => new byte[] { 37, 39 };

    [Test]
    public void Set_Get_Delete_Get_AnotherSet()
    {
        Span<byte> span = stackalloc byte[FixedMap.MinSize];
        var map = new FixedMap(span);

        map.TrySet(FixedMap.Key.Account(Key0), Data0).Should().BeTrue();

        map.TryGet(FixedMap.Key.Account(Key0), out var retrieved).Should().BeTrue();
        Data0.SequenceEqual(retrieved).Should().BeTrue();

        map.Delete(FixedMap.Key.Account(Key0)).Should().BeTrue("Should find and delete entry");
        map.TryGet(FixedMap.Key.Account(Key0), out _).Should().BeFalse("The entry shall no longer exist");

        // should be ready to accept some data again

        map.TrySet(FixedMap.Key.Account(Key1), Data1).Should().BeTrue("Should have memory after previous delete");

        map.TryGet(FixedMap.Key.Account(Key1), out retrieved).Should().BeTrue();
        Data1.SequenceEqual(retrieved).Should().BeTrue();
    }

    [Test]
    public void Defragment_when_no_more_space()
    {
        // by trial and error, found the smallest value that will allow to put these two
        Span<byte> span = stackalloc byte[40];
        var map = new FixedMap(span);

        map.TrySet(FixedMap.Key.Account(Key0), Data0).Should().BeTrue();
        map.TrySet(FixedMap.Key.Account(Key1), Data1).Should().BeTrue();

        map.Delete(FixedMap.Key.Account(Key0)).Should().BeTrue();

        map.TrySet(FixedMap.Key.Account(Key2), Data2).Should().BeTrue("Should retrieve space by running internally the defragmentation");

        // should contains no key0, key1 and key2 now
        map.TryGet(FixedMap.Key.Account(Key0), out var retrieved).Should().BeFalse();

        map.TryGet(FixedMap.Key.Account(Key1), out retrieved).Should().BeTrue();
        Data1.SequenceEqual(retrieved).Should().BeTrue();

        map.TryGet(FixedMap.Key.Account(Key2), out retrieved).Should().BeTrue();
        Data2.SequenceEqual(retrieved).Should().BeTrue();
    }

    [Test]
    public void Update_in_situ()
    {
        // by trial and error, found the smallest value that will allow to put these two
        Span<byte> span = stackalloc byte[24];
        var map = new FixedMap(span);

        map.TrySet(FixedMap.Key.Account(Key1), Data1).Should().BeTrue();
        map.TrySet(FixedMap.Key.Account(Key1), Data2).Should().BeTrue();

        map.TryGet(FixedMap.Key.Account(Key1), out var retrieved).Should().BeTrue();
        Data2.SequenceEqual(retrieved).Should().BeTrue();
    }

    [Test]
    public void Update_in_resize()
    {
        // by trial and error, found the smallest value that will allow to put these two
        Span<byte> span = stackalloc byte[24];
        var map = new FixedMap(span);

        map.TrySet(FixedMap.Key.Account(Key0), Data0).Should().BeTrue();
        map.TrySet(FixedMap.Key.Account(Key0), Data2).Should().BeTrue();

        map.TryGet(FixedMap.Key.Account(Key0), out var retrieved).Should().BeTrue();
        Data2.SequenceEqual(retrieved).Should().BeTrue();
    }


    [Test]
    public void Enumerator()
    {
        Span<byte> span = stackalloc byte[256];
        var map = new FixedMap(span);

        map.TrySet(FixedMap.Key.Account(Key0), Data0);
        map.TrySet(FixedMap.Key.Account(Key1), Data1);
        map.TrySet(FixedMap.Key.Account(Key2), Data2);

        map.Delete(FixedMap.Key.Account(Key1)); // delete K1 to not observe it

        var e = map.GetEnumerator();

        Next(ref e, Key0, Data0);
        Next(ref e, Key2, Data2);

        e.MoveNext().Should().BeFalse();

        static void Next(ref FixedMap.Enumerator e, NibblePath key, ReadOnlySpan<byte> data)
        {
            e.MoveNext().Should().BeTrue();
            e.Current.Path.Equals(key).Should().BeTrue();
            e.Current.Data.SequenceEqual(data).Should().BeTrue();
        }
    }
}