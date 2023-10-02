using FluentAssertions;
using NUnit.Framework;
using Paprika.Crypto;
using Paprika.Data;
using Paprika.Store;

namespace Paprika.Tests.Store;

public class StoreKeyTests
{
    [Test]
    public void Empty()
    {
        const int size = 1;

        var key = Key.Merkle(NibblePath.Empty);

        StoreKey.GetMaxByteSize(key).Should().Be(size);
        var s = StoreKey.Encode(key, stackalloc byte[size]);

        s.Type.Should().Be(DataType.Merkle);

        s.NibbleCount.Should().Be(0);
    }

    [Test]
    public void One_nibble()
    {
        const int size = 1;
        const byte nibble = 0x7;

        var path = NibblePath.FromKey(stackalloc byte[1] { nibble << NibblePath.NibbleShift }).SliceTo(1);
        var key = Key.Merkle(path);

        StoreKey.GetMaxByteSize(key).Should().Be(size);
        var s = StoreKey.Encode(key, stackalloc byte[size]);

        s.Type.Should().Be(key.Type);
        s.GetNibbleAt(0).Should().Be(nibble);

        s.NibbleCount.Should().Be(1);
    }

    [Test]
    public void Two_nibbles()
    {
        const int size = 2;
        const byte nibble0 = 0x7;
        const byte nibble1 = 0x9;
        const int payload = (nibble0 << NibblePath.NibbleShift) | nibble1;

        var path = NibblePath.FromKey(stackalloc byte[1] { (byte)payload });
        var key = Key.Merkle(path);

        StoreKey.GetMaxByteSize(key).Should().Be(size);
        var s = StoreKey.Encode(key, stackalloc byte[size]);

        s.Type.Should().Be(key.Type);
        s.GetNibbleAt(0).Should().Be(nibble0);
        s.GetNibbleAt(1).Should().Be(nibble1);

        s.NibbleCount.Should().Be(2);
    }

    [Test]
    public void Account()
    {
        const int size = Keccak.Size + 1;
        var keccak = NibblePath.FromKey(Values.Key0);

        var key = Key.Account(keccak);

        StoreKey.GetMaxByteSize(key).Should().Be(size);
        var s = StoreKey.Encode(key, stackalloc byte[size]);

        s.Type.Should().Be(key.Type);

        for (var i = 0; i < NibblePath.KeccakNibbleCount; i++)
        {
            s.GetNibbleAt(i).Should().Be(keccak.GetAt(i));
        }

        s.NibbleCount.Should().Be(NibblePath.KeccakNibbleCount);
    }

    [Test]
    public void Merkle_for_storage()
    {
        const int size = Keccak.Size + 1;
        var keccak = NibblePath.FromKey(Values.Key0);
        const byte nibble = 0x7;

        var path = NibblePath.FromKey(stackalloc byte[1] { nibble << NibblePath.NibbleShift }).SliceTo(1);
        var key = Key.Raw(keccak, DataType.Merkle, path);

        StoreKey.GetMaxByteSize(key).Should().Be(size);
        var s = StoreKey.Encode(key, stackalloc byte[size]);

        s.Type.Should().Be(key.Type);

        for (var i = 0; i < NibblePath.KeccakNibbleCount; i++)
        {
            s.GetNibbleAt(i).Should().Be(keccak.GetAt(i));
        }

        s.GetNibbleAt(NibblePath.KeccakNibbleCount).Should().Be(nibble);
        s.NibbleCount.Should().Be(NibblePath.KeccakNibbleCount + 1);
    }

    [Test]
    public void Slice_two_nibbles()
    {
        const int size = 2;
        var path = NibblePath.FromKey(stackalloc byte[1] { 0xA1 });
        var key = Key.Merkle(path);

        StoreKey.GetMaxByteSize(key).Should().Be(size);
        var s = StoreKey.Encode(key, stackalloc byte[size]);

        var sliced = s.SliceTwoNibbles();

        sliced.NibbleCount.Should().Be(0);
    }
}