using FluentAssertions;
using Paprika.Crypto;
using Paprika.Merkle;

namespace Paprika.Tests.Merkle;

public class RlpMemoTests
{
    [Test]
    public void All_children_set_with_all_Keccaks_empty()
    {
        Span<byte> raw = stackalloc byte[RlpMemo.Size];

        Run(raw, 0, NibbleSet.Readonly.All);
    }

    [Test]
    public void All_children_set_with_all_Keccaks_set()
    {
        Span<byte> raw = stackalloc byte[RlpMemo.Size];

        for (var i = 0; i < RlpMemo.Size; i++)
        {
            raw[i] = (byte)(i & 0xFF);
        }

        Run(raw, RlpMemo.Size, NibbleSet.Readonly.All);
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

        Run(raw, RlpMemo.Size - Keccak.Size + NibbleSet.MaxByteSize, NibbleSet.Readonly.All);
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
        raw.Slice(zero0 * Keccak.Size, Keccak.Size).Clear();
        raw.Slice(zero1 * Keccak.Size, Keccak.Size).Clear();

        Run(raw, RlpMemo.Size - 2 * Keccak.Size + NibbleSet.MaxByteSize, NibbleSet.Readonly.All);
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
        Run(raw, size, children);
    }

    [TestCase(0, NibbleSet.NibbleCount - 1)]
    [TestCase(1, 11)]
    public void Two_children_set_with_no_Keccak(int child0, int child1)
    {
        Span<byte> raw = stackalloc byte[RlpMemo.Size];

        var children = new NibbleSet((byte)child0, (byte)child1);

        // just empty map encoded 
        Run(raw, 0, children);
    }

    private static void Run(Span<byte> memoRaw, int compressedSize, NibbleSet.Readonly children)
    {
        var memo = new RlpMemo(memoRaw);

        Span<byte> writeTo = stackalloc byte[compressedSize];
        var written = RlpMemo.Compress(memo.Raw, children, writeTo);
        written.Should().Be(compressedSize);

        var decompressed = RlpMemo.Decompress(writeTo, children, stackalloc byte[RlpMemo.Size]);

        decompressed.Raw.SequenceEqual(memoRaw).Should().BeTrue();
    }
}