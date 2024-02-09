using FluentAssertions;
using NUnit.Framework;
using Paprika.Crypto;
using Paprika.Data;
using static Paprika.Store.RootPage;

namespace Paprika.Tests.Store;

public class RootPageTests
{
    [Test]
    public void Encode_length()
    {
        const byte merkle = 0x02;
        const byte odd = 0x01;

        var path = NibblePath.Parse("01234567");

        var n0 = path.GetAt(0);
        var n0Shifted = (byte)(n0 << NibblePath.NibbleShift);

        var n1 = path.GetAt(1);

        var n2 = path.GetAt(2);
        var n2Shifted = (byte)(n2 << NibblePath.NibbleShift);

        var n3 = path.GetAt(3);

        var n4 = path.GetAt(4);
        var n4Shifted = (byte)(n4 << NibblePath.NibbleShift);

        // length: 0
        Check(NibblePath.Empty, DataType.Merkle, []);

        // length: 1
        Check(path.SliceTo(1), DataType.Merkle, [(byte)(n0Shifted | merkle | odd)]);
        Check(path.SliceTo(1), DataType.Account, [(byte)(n0Shifted | odd)]);
        Check(path.SliceTo(1), DataType.StorageCell, [(byte)(n0Shifted | odd)]);

        // length: 2
        Check(path.SliceTo(2), DataType.Merkle, [(byte)(n0Shifted | n1), merkle]);
        Check(path.SliceTo(2), DataType.Account, [(byte)(n0Shifted | n1), 0]);
        Check(path.SliceTo(2), DataType.StorageCell, [(byte)(n0Shifted | n1), 0]);

        // length: 3
        Check(path.SliceTo(3), DataType.Merkle, [(byte)(n0Shifted | n1), (byte)(n2Shifted | merkle | odd)]);
        Check(path.SliceTo(3), DataType.Account, [(byte)(n0Shifted | n1), (byte)(n2Shifted | odd)]);
        Check(path.SliceTo(3), DataType.StorageCell, [(byte)(n0Shifted | n1), (byte)(n2Shifted | odd)]);

        // length: 4
        Check(path.SliceTo(4), DataType.Merkle, [(byte)(n0Shifted | n1), (byte)(n2Shifted | n3), merkle]);
        Check(path.SliceTo(4), DataType.Account, [(byte)(n0Shifted | n1), (byte)(n2Shifted | n3), 0]);
        Check(path.SliceTo(4), DataType.StorageCell, [(byte)(n0Shifted | n1), (byte)(n2Shifted | n3), 0]);

        // length: 5
        Check(path.SliceTo(5), DataType.Merkle, [(byte)(n0Shifted | n1), (byte)(n2Shifted | n3), (byte)(n4Shifted | merkle | odd)]);
        Check(path.SliceTo(5), DataType.Account, [(byte)(n0Shifted | n1), (byte)(n2Shifted | n3), (byte)(n4Shifted | odd)]);
        Check(path.SliceTo(5), DataType.StorageCell, [(byte)(n0Shifted | n1), (byte)(n2Shifted | n3), (byte)(n4Shifted | odd)]);

        return;

        static void Check(in NibblePath path, DataType type, params byte[] expected)
        {
            var actual = Encode(path, stackalloc byte[64], type);
            (actual.Length % 2).Should().Be(0, "Only even lengths");
            if (actual.RawSpan.SequenceEqual(expected) == false)
            {
                Assert.Fail(
                    $"Mismatch, expected was: {expected.AsSpan().ToHexString(false)} while actual {actual.RawSpan.ToHexString(false)}");
            }
        }
    }
}