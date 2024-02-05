using FluentAssertions;
using NUnit.Framework;
using Paprika.Crypto;
using Paprika.Data;

namespace Paprika.Tests.Data;

public class KeyTests
{
    private const string Path = nameof(Key.Path);
    private const string Type = nameof(Key.Type);
    private const string AdditionalKey = nameof(Key.StoragePath);

    private static NibblePath Path0 => NibblePath.FromKey(Values.Key0);
    private static NibblePath Path1A => NibblePath.FromKey(Values.Key1A);
    private static NibblePath Path1B => NibblePath.FromKey(Values.Key1B);

    [Test]
    public void Equality_Path()
    {
        Key.Account(Path0).Equals(Key.Account(Path0)).Should().BeTrue($"Same {Path} & {Type}");

        Key.Account(Path0).Equals(Key.Account(Path1A)).Should().BeFalse($"{Path} different at first nibble");

        Key.Account(Path1A).Equals(Key.Account(Path1B)).Should().BeFalse($"{Path} different at last nibble");

        const int length = 63;
        Key.Account(Path1A.SliceTo(length)).Equals(Key.Account(Path1B.SliceTo(length))).Should()
            .BeTrue($"Truncated {Path} on different nibble should be equal");
    }

    [Test]
    public void Equality_Type()
    {
        Key.Account(Path0).Equals(Key.StorageCell(Path0, Keccak.Zero)).Should().BeFalse($"Different {Type}");
    }

    [Test]
    public void Equality_AdditionalKey()
    {
        Key.StorageCell(Path0, Values.Key1A).Equals(Key.StorageCell(Path0, Values.Key1A)).Should()
            .BeTrue($"Same {Path} & {Type} & {AdditionalKey}");

        Key.StorageCell(Path0, Values.Key1A).Equals(Key.StorageCell(Path0, Values.Key1B)).Should()
            .BeFalse($"Same {Path} & {Type} but different {AdditionalKey}");
    }

    [Test]
    public void Serialization_Account() => ReadWriteAssert(Key.Account(NibblePath.Empty));

    [Test]
    public void Serialization_Storage() => ReadWriteAssert(Key.StorageCell(Path0, Values.Key1A));

    [Test]
    public void LongHash()
    {
        Keccak path0 = default;

        Keccak path1 = default;
        path1.BytesAsSpan[31] = 0x01;

        Keccak path2 = default;
        path2.BytesAsSpan[31] = 0x02;

        var hashes = new Dictionary<ulong, string>();

        // account
        var account = Keccak.EmptyTreeHash;
        var aPath = NibblePath.FromKey(account);

        Unique(Key.Account(account));

        // storage
        Unique(Key.StorageCell(aPath, path0));
        Unique(Key.StorageCell(aPath, path1));
        Unique(Key.StorageCell(aPath, path2));

        // Merkle, all the account prefixes
        for (var i = 0; i < 32; i++)
        {
            Unique(Key.Merkle(aPath.SliceTo(i)));
        }

        // Merkle, branches of 000 addresses
        var zero = NibblePath.FromKey(Keccak.Zero);

        // Start from 1st as the root is covered above
        for (var i = 1; i < 32; i++)
        {
            Unique(Key.Merkle(zero.SliceTo(i)));
        }

        // Merkle, all the storage prefixes
        for (var i = 0; i < 32; i++)
        {
            Unique(Key.Raw(aPath, DataType.Merkle,
                NibblePath.FromKey(Keccak.EmptyTreeHash).SliceTo(i)));
        }

        return;

        void Unique(in Key key)
        {
            var hex = key.ToString();

            var hash = key.GetHashCodeULong();
            if (hashes.TryAdd(hash, hex) == false)
            {
                var existing = hashes[hash];
                Assert.Fail($"The hash for {hex} is the same as for {existing}");
            }
        }
    }


    private static void ReadWriteAssert(in Key expected)
    {
        var written = expected.WriteTo(stackalloc byte[expected.MaxByteLength]);
        Key.ReadFrom(written, out var actual);
        actual.Equals(expected).Should().BeTrue();
    }
}