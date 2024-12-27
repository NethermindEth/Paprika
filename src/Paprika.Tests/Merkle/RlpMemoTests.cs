using FluentAssertions;
using Paprika.Crypto;
using Paprika.Data;
using Paprika.Merkle;
using Paprika.RLP;

namespace Paprika.Tests.Merkle;

public class RlpMemoTests
{

    // All the write operations on RlpMemo
    private enum RlpMemoOperation
    {
        Set,
        SetRaw,
        Clear,
        Delete,
        Insert
    }

    [Test]
    public void Random_delete()
    {
        Span<byte> raw = stackalloc byte[RlpMemo.MaxSize];
        var children = new NibbleSet();

        for (var i = 0; i < RlpMemo.MaxSize; i++)
        {
            raw[i] = (byte)(i & 0xFF);
        }

        for (var i = 0; i < NibbleSet.NibbleCount; i++)
        {
            children[(byte)i] = true;
        }

        var memo = new RlpMemo(raw);
        var rand = new Random(13);

        for (var i = 0; i < NibbleSet.NibbleCount; i++)
        {
            var child = (byte)rand.Next(NibbleSet.NibbleCount);

            while (children[child] == false)
            {
                child = (byte)rand.Next(NibbleSet.NibbleCount);
            }

            children[child] = false;
            memo = RlpMemo.Delete(memo, child, children, raw);

            memo.Length.Should().Be(RlpMemo.MaxSize - (i + 1) * Keccak.Size);
            memo.Exists(child, children).Should().BeFalse();
            memo.TryGetKeccak(child, out var keccak, children).Should().BeFalse();
            keccak.IsEmpty.Should().BeTrue();
        }

        memo.Length.Should().Be(0);
    }

    [Test]
    public void Random_insert()
    {
        Span<byte> raw = [];
        Span<byte> workingMemory = new byte[RlpMemo.MaxSize];
        var children = new NibbleSet();

        Span<byte> keccak = new byte[Keccak.Size];
        keccak.Fill(0xFF);

        for (var i = 0; i < NibbleSet.NibbleCount; i++)
        {
            children[(byte)i] = false;
        }

        var memo = new RlpMemo(raw);
        var rand = new Random(13);

        for (var i = 0; i < NibbleSet.NibbleCount; i++)
        {
            var child = (byte)rand.Next(NibbleSet.NibbleCount);

            while (children[child])
            {
                child = (byte)rand.Next(NibbleSet.NibbleCount);
            }

            children[child] = true;
            memo = RlpMemo.Insert(memo, child, children, keccak, workingMemory);

            memo.Length.Should().Be((i + 1) * Keccak.Size);
            memo.Exists(child, children).Should().BeTrue();
            memo.TryGetKeccak(child, out var k, children).Should().BeTrue();
            k.SequenceEqual(keccak).Should().BeTrue();
        }

        memo.Length.Should().Be(RlpMemo.MaxSize);
    }

    [Test]
    public void In_place_update()
    {
        Span<byte> raw = stackalloc byte[RlpMemo.MaxSize];

        for (var i = 0; i < RlpMemo.MaxSize; i++)
        {
            raw[i] = (byte)(i & 0xFF);
        }

        var memo = new RlpMemo(raw);

        var children = new NibbleSet();
        for (var i = 0; i < NibbleSet.NibbleCount; i++)
        {
            children[(byte)i] = true;
        }

        // Delete each child and the corresponding keccak
        for (byte i = 0; i < NibbleSet.NibbleCount; i++)
        {
            children[i] = false;
            memo = RlpMemo.Delete(memo, i, children, raw);

            memo.Length.Should().Be(RlpMemo.MaxSize - (i + 1) * Keccak.Size);
            memo.Exists(i, children).Should().BeFalse();
            memo.TryGetKeccak(i, out var k, children).Should().BeFalse();
            k.IsEmpty.Should().BeTrue();
        }

        memo.Length.Should().Be(0);

        // Try adding back the children and the corresponding keccak
        Span<byte> keccak = stackalloc byte[Keccak.Size];
        keccak.Fill(0xFF);

        for (byte i = 0; i < NibbleSet.NibbleCount; i++)
        {
            children[i] = true;
            memo = RlpMemo.Insert(memo, i, children, keccak, raw);

            memo.Length.Should().Be((i + 1) * Keccak.Size);
            memo.Exists(i, children).Should().BeTrue();
            memo.TryGetKeccak(i, out var k, children).Should().BeTrue();
            k.SequenceEqual(keccak).Should().BeTrue();
        }

        memo.Length.Should().Be(RlpMemo.MaxSize);
    }

    [Test]
    public void Copy_data()
    {
        Span<byte> raw = stackalloc byte[RlpMemo.MaxSize];
        Span<byte> rawCopy = stackalloc byte[RlpMemo.MaxSize];

        for (var i = 0; i < RlpMemo.MaxSize; i++)
        {
            raw[i] = (byte)(i & 0xFF);
        }

        var memo = new RlpMemo(raw);
        memo = RlpMemo.Copy(memo.Raw, rawCopy);
        memo.Raw.SequenceEqual(raw).Should().BeTrue();
    }

    [TestCase(1000)]
    [TestCase(10_000)]
    [TestCase(100_000)]
    public void Large_random_operations(int numOperations)
    {
        Span<byte> raw = stackalloc byte[RlpMemo.MaxSize];
        var children = new NibbleSet();

        Span<byte> keccak = new byte[Keccak.Size];
        keccak.Fill(0xFF);
        KeccakOrRlp.FromSpan(keccak, out var keccakOrRlp);

        for (var i = 0; i < RlpMemo.MaxSize; i++)
        {
            raw[i] = (byte)(i & 0xFF);
        }

        for (var i = 0; i < NibbleSet.NibbleCount; i++)
        {
            children[(byte)i] = false;
        }

        var memo = new RlpMemo(raw);
        var rand = new Random(13);

        for (var i = 0; i < numOperations; i++)
        {
            var child = (byte)rand.Next(NibbleSet.NibbleCount);
            var op = (RlpMemoOperation)rand.Next(Enum.GetValues<RlpMemoOperation>().Length);

            switch (op)
            {
                case RlpMemoOperation.Set:
                    if (memo.Exists(child, children))
                    {
                        memo.Set(keccakOrRlp, child, children);

                        memo.TryGetKeccak(child, out var k, children).Should().BeTrue();
                        k.SequenceEqual(keccakOrRlp.Span).Should().BeTrue();
                    }

                    break;
                case RlpMemoOperation.SetRaw:
                    if (memo.Exists(child, children))
                    {
                        memo.SetRaw(keccak, child, children);

                        memo.TryGetKeccak(child, out var k, children).Should().BeTrue();
                        k.SequenceEqual(keccak).Should().BeTrue();
                    }

                    break;
                case RlpMemoOperation.Clear:
                    if (memo.Exists(child, children))
                    {
                        memo.Clear(child, children);

                        memo.TryGetKeccak(child, out var k, children).Should().BeFalse();
                        k.IsEmpty.Should().BeTrue();
                    }

                    break;
                case RlpMemoOperation.Delete:
                    if (memo.Exists(child, children))
                    {
                        children[child] = false;
                        memo = RlpMemo.Delete(memo, child, children, raw);

                        memo.TryGetKeccak(child, out var k, children).Should().BeFalse();
                        k.IsEmpty.Should().BeTrue();
                    }

                    break;
                case RlpMemoOperation.Insert:
                    if (!memo.Exists(child, children))
                    {
                        children[child] = true;
                        memo = RlpMemo.Insert(memo, child, children, keccak, raw);

                        memo.TryGetKeccak(child, out var k, children).Should().BeTrue();
                        k.SequenceEqual(keccak).Should().BeTrue();
                    }

                    break;
            }
        }
    }
}
