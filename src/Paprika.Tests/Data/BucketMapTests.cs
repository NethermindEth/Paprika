using FluentAssertions;
using NUnit.Framework;
using Paprika.Crypto;
using Paprika.Data;

namespace Paprika.Tests.Data;

public class BucketMapTests
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
    public void Clear()
    {
        Span<byte> span = stackalloc byte[BucketMap.TotalSize];
        Random.Shared.NextBytes(span);

        var map = new BucketMap(span);

        map.Clear();

        for (byte nibble = 0; nibble < BucketMap.BucketCount; nibble++)
        {
            map.TryGetByNibble(nibble, out _, out _).Should().BeFalse();
        }
    }
    
    [Test]
    public void Set()
    {
        var data = new byte[51];
        
        Span<byte> span = stackalloc byte[BucketMap.TotalSize];
        var map = new BucketMap(span);

        var key = Key.Account(Key0);
        map.Set(key, data);

        var set = Key0.FirstNibble;

        for (byte nibble = 0; nibble < BucketMap.BucketCount; nibble++)
        {
            if (nibble != set)
            {
                map.TryGetByNibble(nibble, out _, out _).Should().BeFalse();
            }
            else
            {
                map.TryGetByNibble(nibble, out var actualKey, out var actualData);

                key.Equals(actualKey).Should().BeTrue();
                data.AsSpan().SequenceEqual(actualData).Should().BeTrue();
            }
        }
    }
    
    [Test]
    public void Set_overwrites()
    {
        var data = new byte[51];
        
        Span<byte> span = stackalloc byte[BucketMap.TotalSize];
        var map = new BucketMap(span);

        var key = Key.StorageCell(Key1, StorageCell0);
        map.Set(Key.Account(Key0), new byte[] { 13, 17, 21 });
        map.Set(key, data);

        var set = key.Path.FirstNibble;

        for (byte nibble = 0; nibble < BucketMap.BucketCount; nibble++)
        {
            if (nibble != set)
            {
                map.TryGetByNibble(nibble, out _, out _).Should().BeFalse();
            }
            else
            {
                map.TryGetByNibble(nibble, out var actualKey, out var actualData);

                key.Equals(actualKey).Should().BeTrue();
                data.AsSpan().SequenceEqual(actualData).Should().BeTrue();
            }
        }
    }
}