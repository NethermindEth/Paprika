using System.Buffers.Binary;
using FluentAssertions;
using NUnit.Framework;
using Paprika.Store;

namespace Paprika.Tests.Store;

public class IdTests
{
    [Test]
    public void Small()
    {
        var unique = new HashSet<byte>();

        for (uint i = 0; i < Id.Limit; i++)
        {
            var encoded = Id.WriteId(i);
            encoded.Length.Should().Be(1);
            unique.Add(encoded[0]).Should().BeTrue();
        }
    }

    [Test]
    public void Bigger()
    {
        var unique = new HashSet<ushort>();

        for (uint i = Id.Limit; i < Id.Limit * 128; i++)
        {
            var encoded = Id.WriteId(i);
            encoded.Length.Should().Be(2);
            unique.Add(BinaryPrimitives.ReadUInt16LittleEndian(encoded)).Should().BeTrue();
        }
    }

    [Test]
    public void Big()
    {
        var unique = new HashSet<uint>();

        const uint start = 1 * 256 * 256 * 256;
        for (uint i = start; i < start + 1_000; i++)
        {
            var encoded = Id.WriteId(i);
            encoded.Length.Should().Be(4);
            unique.Add(BinaryPrimitives.ReadUInt32LittleEndian(encoded)).Should().BeTrue();
        }
    }
}