using System.Buffers.Binary;
using System.Diagnostics;
using FluentAssertions;
using Nethermind.Int256;
using NUnit.Framework;
using Paprika.Crypto;
using Paprika.Data;
using Paprika.Merkle;
using Paprika.Store;

namespace Paprika.Tests.Store;

public class DataPageTests : BasePageTests
{
    private const uint BatchId = 1;

    [DebuggerStepThrough]
    private static byte[] GetValue(int i) => new UInt256((uint)i).ToBigEndian();

    [Test]
    public void Spinning_through_same_keys_should_use_limited_number_of_pages()
    {
        var batch = NewBatch(BatchId);
        var data = ((IBatchContext)batch).GetNewPage<DataPage>(out _);

        const int spins = 100;
        const int count = 1024;

        for (var spin = 0; spin < spins; spin++)
        {
            for (var i = 0; i < count; i++)
            {
                Keccak keccak = default;
                BinaryPrimitives.WriteInt32LittleEndian(keccak.BytesAsSpan, i);
                var path = NibblePath.FromKey(keccak);

                data = new DataPage(data.Set(path, GetValue(i), batch));
            }

            batch = batch.Next();
        }

        for (var j = 0; j < count; j++)
        {
            Keccak search = default;
            BinaryPrimitives.WriteInt32LittleEndian(search.BytesAsSpan, j);

            data.TryGet(batch, NibblePath.FromKey(search), out var result)
                .Should()
                .BeTrue($"Failed to read {j}");

            result.SequenceEqual(GetValue(j))
                .Should()
                .BeTrue($"Failed to read value of {j}");
        }

        batch.PageCount.Should().BeLessThan(500);
    }

    [TestCase(0)]
    [TestCase(1)]
    public void Should_hold_all_needed_full_branches_of_Merkle(byte oddity)
    {
        var batch = NewBatch(BatchId);
        var data = ((IBatchContext)batch).GetNewPage<DataPage>(out _);

        var path = NibblePath.Empty;
        data.Set(path, GetValue(path), batch);

        const int count = 16;
        for (byte i = 0; i < count; i++)
        {
            path = NibblePath.Single(i, oddity);
            data.Set(path, GetValue(path), batch);
        }

        for (byte i = 0; i < count; i++)
        {
            path = NibblePath.Single(i, oddity);
            data.TryGet(batch, path, out var result).Should().BeTrue();
            result.SequenceEqual(GetValue(path)).Should().BeTrue();
        }

        batch.PageCount.Should().Be(3, "One for the data page and two for the Merkle children");

        return;
        static byte[] GetValue(in NibblePath path)
        {
            const int someOverhead = 32;
            var bytes = new byte[RlpMemo.MaxSize + Node.Branch.MaxByteLength + someOverhead];

            path.WriteTo(bytes);

            return bytes;
        }
    }
}