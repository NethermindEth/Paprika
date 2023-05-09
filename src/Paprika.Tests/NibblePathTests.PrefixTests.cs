using NUnit.Framework;
using Paprika.Db;

namespace Paprika.Tests;

[TestFixture(true)]
[TestFixture(false)]
public class NibblePathHexPrefixTests
{
    private readonly bool _odd;
    private readonly int _shift;

    public NibblePathHexPrefixTests(bool odd)
    {
        _odd = odd;
        _shift = odd ? 0 : NibblePath.NibbleShift;
    }

    [TestCase(false, (byte)3, (byte)19)]
    [TestCase(true, (byte)3, (byte)51)]
    public void Encode_gives_correct_output_when_one(bool flag, byte nibble1, byte byte1)
    {
        Span<byte> span = stackalloc byte[1];
        span[0] = (byte)(nibble1 << _shift);

        var path = _odd ? NibblePath.FromKey(span, 1) : NibblePath.FromKey(span).SliceTo(1);

        Span<byte> destination = stackalloc byte[path.HexEncodedLength];

        path.HexEncode(destination, flag);
        Assert.AreEqual(1, destination.Length);
        Assert.AreEqual(byte1, destination[0]);
    }

    [TestCase(false, (byte)3, (byte)7, (byte)13, (byte)19, (byte)125)]
    [TestCase(true, (byte)3, (byte)7, (byte)13, (byte)51, (byte)125)]
    public void Encode_gives_correct_output_when_odd(bool flag, byte nibble1, byte nibble2, byte nibble3,
        byte byte1, byte byte2)
    {
        NibblePath path;
        if (_odd)
        {
            var key = new[] { nibble1, (byte)((nibble2 << NibblePath.NibbleShift) | nibble3) };
            path = NibblePath.FromKey(key, 1);
        }
        else
        {
            var key = new[] { (byte)((nibble1 << NibblePath.NibbleShift) | nibble2), (byte)(nibble3 << NibblePath.NibbleShift) };
            path = NibblePath.FromKey(key).SliceTo(3);
        }

        Span<byte> destination = stackalloc byte[path.HexEncodedLength];
        path.HexEncode(destination, flag);

        Assert.AreEqual(2, path.HexEncodedLength);
        Assert.AreEqual(byte1, destination[0]);
        Assert.AreEqual(byte2, destination[1]);
    }

    [TestCase(false, (byte)3, (byte)7, (byte)0, (byte)55)]
    [TestCase(true, (byte)3, (byte)7, (byte)32, (byte)55)]
    public void Encode_gives_correct_output_when_even(bool flag, byte nibble1, byte nibble2, byte byte1, byte byte2)
    {
        NibblePath path;
        if (_odd)
        {
            var key = new[] { nibble1, (byte)(nibble2 << NibblePath.NibbleShift) };
            path = NibblePath.FromKey(key, 1).SliceTo(2);
        }
        else
        {
            var key = new[] { (byte)((nibble1 << NibblePath.NibbleShift) | nibble2) };
            path = NibblePath.FromKey(key);
        }

        Span<byte> destination = stackalloc byte[path.HexEncodedLength];

        path.HexEncode(destination, flag);

        Assert.AreEqual(2, path.HexEncodedLength);
        Assert.AreEqual(byte1, destination[0]);
        Assert.AreEqual(byte2, destination[1]);
    }

    // [TestCase(false, (byte)3, (byte)7, (byte)0, (byte)55)]
    // [TestCase(true, (byte)3, (byte)7, (byte)32, (byte)55)]
    // public void Decode_gives_correct_output_when_even(bool expectedFlag, byte nibble1, byte nibble2, byte byte1,
    //     byte byte2)
    // {
    //     (byte[] key, bool isLeaf) = HexPrefix.FromBytes(new[] { byte1, byte2 });
    //     Assert.AreEqual(expectedFlag, isLeaf);
    //     Assert.AreEqual(2, key.Length);
    //     Assert.AreEqual(nibble1, key[0]);
    //     Assert.AreEqual(nibble2, key[1]);
    // }
    //
    // [TestCase(false, (byte)3, (byte)19)]
    // [TestCase(true, (byte)3, (byte)51)]
    // public void Decode_gives_correct_output_when_one(bool expectedFlag, byte nibble1, byte byte1)
    // {
    //     (byte[] key, bool isLeaf) = HexPrefix.FromBytes(new[] { byte1 });
    //
    //     Assert.AreEqual(expectedFlag, isLeaf);
    //     Assert.AreEqual(1, key.Length);
    //     Assert.AreEqual(nibble1, key[0]);
    // }
    //
    // [TestCase(false, (byte)3, (byte)7, (byte)13, (byte)19, (byte)125)]
    // [TestCase(true, (byte)3, (byte)7, (byte)13, (byte)51, (byte)125)]
    // public void Decode_gives_correct_output_when_odd(bool expectedFlag, byte nibble1, byte nibble2, byte nibble3,
    //     byte byte1, byte byte2)
    // {
    //     (byte[] key, bool isLeaf) = HexPrefix.FromBytes(new[] { byte1, byte2 });
    //     Assert.AreEqual(expectedFlag, isLeaf);
    //     Assert.AreEqual(3, key.Length);
    //     Assert.AreEqual(nibble1, key[0]);
    //     Assert.AreEqual(nibble2, key[1]);
    //     Assert.AreEqual(nibble3, key[2]);
    // }
}