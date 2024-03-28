using FluentAssertions;
using Paprika.Crypto;
using Paprika.Data;
using Paprika.Merkle;

namespace Paprika.Tests.Merkle;

public class RlpMemoTests
{
    public const bool OddKey = true;
    public const bool EvenKey = false;

    [Test]
    public void All_children_set_with_all_Keccaks_empty()
    {
        Span<byte> raw = stackalloc byte[RlpMemo.Size];

        Run(raw, 0, NibbleSet.Readonly.All, OddKey);
    }

    [Test]
    public void All_children_set_with_all_Keccaks_set()
    {
        Span<byte> raw = stackalloc byte[RlpMemo.Size];

        for (var i = 0; i < RlpMemo.Size; i++)
        {
            raw[i] = (byte)(i & 0xFF);
        }

        Run(raw, RlpMemo.Size, NibbleSet.Readonly.All, OddKey);
    }

    [TestCase(0)]
    [TestCase(1)]
    [TestCase(3)]
    [TestCase(11)]
    [TestCase(NibbleSet.NibbleCount - 1)]
    public void All_children_set_with_one_zero(int zero)
    {
        Span<byte> raw = stackalloc byte[RlpMemo.Size];

        for (var i = 0; i < RlpMemo.Size; i++)
        {
            raw[i] = (byte)(i & 0xFF);
        }

        // zero one
        raw.Slice(zero * Keccak.Size, Keccak.Size).Clear();

        Run(raw, RlpMemo.Size - Keccak.Size + NibbleSet.MaxByteSize, NibbleSet.Readonly.All, OddKey);
    }

    [TestCase(0, NibbleSet.NibbleCount - 1)]
    [TestCase(1, 11)]
    public void All_children_set_with_two_zeros(int zero0, int zero1)
    {
        Span<byte> raw = stackalloc byte[RlpMemo.Size];

        for (var i = 0; i < RlpMemo.Size; i++)
        {
            raw[i] = (byte)(i & 0xFF);
        }

        // clear zeroes
        var memo = new RlpMemo(raw);
        memo.Clear((byte)zero0);
        memo.Clear((byte)zero1);

        Run(raw, RlpMemo.Size - 2 * Keccak.Size + NibbleSet.MaxByteSize, NibbleSet.Readonly.All, OddKey);
    }

    [TestCase(0, NibbleSet.NibbleCount - 1)]
    [TestCase(1, 11)]
    public void Two_children_set_with_Keccak(int child0, int child1)
    {
        Span<byte> raw = stackalloc byte[RlpMemo.Size];

        // fill set
        raw.Slice(child0 * Keccak.Size, Keccak.Size).Fill(1);
        raw.Slice(child1 * Keccak.Size, Keccak.Size).Fill(17);

        var children = new NibbleSet((byte)child0, (byte)child1);

        // none of the children set is empty, keccaks encoded without the empty map 
        const int size = 2 * Keccak.Size;
        Run(raw, size, children, OddKey);
    }

    [TestCase(1, 11)]
    public void Two_children_on_even_level_have_no_memo(int child0, int child1)
    {
        Span<byte> raw = stackalloc byte[RlpMemo.Size];

        // fill set
        raw.Slice(child0 * Keccak.Size, Keccak.Size).Fill(1);
        raw.Slice(child1 * Keccak.Size, Keccak.Size).Fill(17);

        var children = new NibbleSet((byte)child0, (byte)child1);

        Run(raw, 0, children, EvenKey);
    }

    [TestCase(0, NibbleSet.NibbleCount - 1)]
    [TestCase(1, 11)]
    public void Two_children_set_with_no_Keccak(int child0, int child1)
    {
        Span<byte> raw = stackalloc byte[RlpMemo.Size];

        var children = new NibbleSet((byte)child0, (byte)child1);

        // just empty map encoded 
        Run(raw, 0, children, OddKey);
    }

    private static void Run(Span<byte> memoRaw, int compressedSize, NibbleSet.Readonly children, bool oddKey)
    {
        var isOdd = oddKey == OddKey;

        var key = Key.Merkle(isOdd ? NibblePath.Single(1, 0) : NibblePath.Empty);
        var memo = new RlpMemo(memoRaw);

        Span<byte> writeTo = stackalloc byte[compressedSize];
        var written = RlpMemo.Compress(key, memo.Raw, children, writeTo);
        written.Should().Be(compressedSize);

        if (!isOdd && children.SetCount == 2)
        {
            // cleanup
            var decompressed = RlpMemo.Decompress(writeTo, children, stackalloc byte[RlpMemo.Size]);
            decompressed.Raw.SequenceEqual(RlpMemo.Empty).Should().BeTrue();
        }
        else
        {
            var decompressed = RlpMemo.Decompress(writeTo, children, stackalloc byte[RlpMemo.Size]);
            decompressed.Raw.SequenceEqual(memoRaw).Should().BeTrue();
        }
    }
}