using FluentAssertions;
using Paprika.Crypto;
using Paprika.Merkle;
using Paprika.Data;
using Paprika.Chain;

namespace Paprika.Tests.Merkle;

public class RlpMemoTests
{
    // All the write operations on RlpMemo
    private enum RlpMemoOperation
    {
        Set,
        Clear,
        Delete,
        Insert
    }

    [Test]
    public void Insert_get_operation()
    {
        Span<byte> workingMemory = new byte[RlpMemo.MaxSize];
        NibbleSet.Readonly children = new NibbleSet(0xA, 0xB, 0xC);
        var memo = new RlpMemo([]);

        InsertRandomKeccak(ref memo, children, out var data, workingMemory);

        memo.Length.Should().Be(GetExpectedSize(children.SetCount));

        for (byte i = 0; i < NibbleSet.NibbleCount; i++)
        {
            if (children[i])
            {
                memo.Exists(i).Should().BeTrue();
                memo.TryGetKeccak(i, out var k).Should().BeTrue();
                k.SequenceEqual(data[i].Span).Should().BeTrue();
            }
        }

        CompareMemoAndDict(memo, data);
    }

    [Test]
    public void Set_get_operation()
    {
        Span<byte> workingMemory = new byte[RlpMemo.MaxSize];
        NibbleSet.Readonly children = new NibbleSet(0xA, 0xB, 0xC);
        var memo = new RlpMemo([]);

        InsertRandomKeccak(ref memo, children, out var data, workingMemory);

        memo.Length.Should().Be(GetExpectedSize(children.SetCount));

        var random = new Random(13);

        for (byte i = 0; i < NibbleSet.NibbleCount; i++)
        {
            if (children[i])
            {
                data[i] = random.NextKeccak();
                memo.Set(data[i].Span, i);
                CompareMemoAndDict(memo, data);
            }
        }

        memo.Length.Should().Be(GetExpectedSize(children.SetCount));

        for (byte i = 0; i < NibbleSet.NibbleCount; i++)
        {
            if (children[i])
            {
                memo.Exists(i).Should().BeTrue();
                memo.TryGetKeccak(i, out var k).Should().BeTrue();
                k.SequenceEqual(data[i].Span).Should().BeTrue();
            }
        }

        CompareMemoAndDict(memo, data);
    }

    [Test]
    public void Clear_get_operation()
    {
        Span<byte> workingMemory = new byte[RlpMemo.MaxSize];
        NibbleSet.Readonly children = new NibbleSet(0xA, 0xB, 0xC);
        var memo = new RlpMemo([]);

        InsertRandomKeccak(ref memo, children, out var data, workingMemory);

        memo.Length.Should().Be(GetExpectedSize(children.SetCount));

        for (byte i = 0; i < NibbleSet.NibbleCount; i++)
        {
            if (children[i])
            {
                data[i] = Keccak.Zero;
                memo.Clear(i);
                CompareMemoAndDict(memo, data);
            }
        }

        memo.Length.Should().Be(GetExpectedSize(children.SetCount));

        for (byte i = 0; i < NibbleSet.NibbleCount; i++)
        {
            if (children[i])
            {
                memo.Exists(i).Should().BeTrue();
                memo.TryGetKeccak(i, out var k).Should().BeFalse();
                k.IsEmpty.Should().BeTrue();
            }
        }

        CompareMemoAndDict(memo, data);
    }

    [Test]
    public void Delete_get_operation()
    {
        Span<byte> workingMemory = new byte[RlpMemo.MaxSize];
        NibbleSet.Readonly children = new NibbleSet(0xA, 0xB, 0xC);
        var memo = new RlpMemo([]);

        InsertRandomKeccak(ref memo, children, out var data, workingMemory);

        memo.Length.Should().Be(GetExpectedSize(children.SetCount));

        for (byte i = 0; i < NibbleSet.NibbleCount; i++)
        {
            if (children[i])
            {
                data.Remove(i);
                memo = RlpMemo.Delete(memo, i, workingMemory);
                CompareMemoAndDict(memo, data);
            }
        }

        memo.Length.Should().Be(0);

        for (byte i = 0; i < NibbleSet.NibbleCount; i++)
        {
            if (children[i])
            {
                memo.Exists(i).Should().BeFalse();
                memo.TryGetKeccak(i, out var k).Should().BeFalse();
                k.IsEmpty.Should().BeTrue();
            }
        }

        CompareMemoAndDict(memo, data);
    }

    [Test]
    public void Random_delete()
    {
        Span<byte> raw = stackalloc byte[RlpMemo.MaxSize];
        var children = new NibbleSet();

        for (var i = 0; i < RlpMemo.MaxSize - NibbleSet.MaxByteSize; i++)
        {
            raw[i] = (byte)(i & 0xFF);
        }

        // Set all the index bits at the end.
        for (var i = RlpMemo.MaxSize - 1; i >= RlpMemo.MaxSize - NibbleSet.MaxByteSize; i--)
        {
            raw[i] = 0xFF;
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
            memo = RlpMemo.Delete(memo, child, raw);

            memo.Length.Should().Be(GetExpectedSize(NibbleSet.NibbleCount - i - 1));
            memo.Exists(child).Should().BeFalse();
            memo.TryGetKeccak(child, out var keccak).Should().BeFalse();
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
            memo = RlpMemo.Insert(memo, child, keccak, workingMemory);

            memo.Length.Should().Be(GetExpectedSize(i + 1));
            memo.Exists(child).Should().BeTrue();
            memo.TryGetKeccak(child, out var k).Should().BeTrue();
            k.SequenceEqual(keccak).Should().BeTrue();
        }

        memo.Length.Should().Be(RlpMemo.MaxSize);
    }

    [Test]
    public void Grow_shrink()
    {
        Span<byte> raw = new byte[RlpMemo.MaxSize];
        var memo = new RlpMemo([]);
        var random = new Random(13);
        var data = new Dictionary<byte, Keccak>();

        // Grow the RLPMemo
        for (byte i = 0; i < NibbleSet.NibbleCount; i++)
        {
            data[i] = random.NextKeccak();
            memo = RlpMemo.Insert(memo, i, data[i].Span, raw[..GetExpectedSize(i + 1)]);
            CompareMemoAndDict(memo, data);
        }

        memo.Length.Should().Be(RlpMemo.MaxSize);

        // Shrink the RLPMemo
        for (byte i = 0; i < NibbleSet.NibbleCount; i++)
        {
            memo = RlpMemo.Delete(memo, i, raw[..GetExpectedSize(NibbleSet.NibbleCount - i - 1)]);
            data.Remove(i);
            CompareMemoAndDict(memo, data);
        }

        memo.Length.Should().Be(0);
    }

    [Test]
    public void Shrink_grow()
    {
        Span<byte> raw = stackalloc byte[RlpMemo.MaxSize];
        var random = new Random(13);
        var data = new Dictionary<byte, Keccak>();

        for (byte i = 0; i < NibbleSet.NibbleCount; i++)
        {
            data[i] = random.NextKeccak();
            data[i].Span.CopyTo(raw[(i * Keccak.Size)..]);
        }

        var memo = new RlpMemo(raw);

        // Shrink the RLPMemo
        for (byte i = 0; i < NibbleSet.NibbleCount; i++)
        {
            memo = RlpMemo.Delete(memo, i, raw[..GetExpectedSize(NibbleSet.NibbleCount - i - 1)]);
            data.Remove(i);
            CompareMemoAndDict(memo, data);
        }

        memo.Length.Should().Be(0);

        // Grow the RLPMemo
        for (byte i = 0; i < NibbleSet.NibbleCount; i++)
        {
            data[i] = random.NextKeccak();
            memo = RlpMemo.Insert(memo, i, data[i].Span, raw[..GetExpectedSize(i + 1)]);
            CompareMemoAndDict(memo, data);
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

    [Test]
    public void Keccak_to_rlp_children()
    {
        NibbleSet.Readonly children = new NibbleSet(1, 2);
        Span<byte> workingMemory = new byte[RlpMemo.MaxSize];

        // Create memo with random keccak for the corresponding children
        var memo = new RlpMemo([]);
        InsertRandomKeccak(ref memo, children, out _, workingMemory);

        // create E->B->L
        //            ->L
        // leaves without any key and very small value cause to be inlined in branch
        // encoded branch rlp is also < 32 bytes which causes it to be encoded as RLP in extension node
        const string prefix = "ccccccccccccccccccccddddddddddddddddeeeeeeeeeeeeeeeeeeeeeeeeeeb";
        Keccak storageKey1 =
            new Keccak(Convert.FromHexString(prefix + "1"));
        Keccak storageKey2 =
            new Keccak(Convert.FromHexString(prefix + "2"));

        var commit = new Commit();
        commit.Set(Key.Account(Values.Key0),
            new Account(0, 1).WriteTo(stackalloc byte[Paprika.Account.MaxByteCount]));
        commit.Set(Key.StorageCell(NibblePath.FromKey(Values.Key0), storageKey1), new byte[] { 1, 2, 3 });
        commit.Set(Key.StorageCell(NibblePath.FromKey(Values.Key0), storageKey2), new byte[] { 10, 20, 30 });

        using var merkle = new ComputeMerkleBehavior();

        merkle.BeforeCommit(commit, CacheBudget.Options.None.Build());

        // Update the branch with memo
        commit.SetBranch(Key.Raw(NibblePath.FromKey(Values.Key0), DataType.Merkle, NibblePath.Parse(prefix)), children,
            memo.Raw);

        merkle.RecalculateStorageTrie(commit, Values.Key0, CacheBudget.Options.None.Build());
    }

    [TestCase(1000)]
    [TestCase(10_000)]
    [TestCase(100_000)]
    public void Large_random_operations(int numOperations)
    {
        Span<byte> workingSet = new byte[RlpMemo.MaxSize];
        var rand = new Random(13);
        var memo = new RlpMemo([]);
        NibbleSet children = new NibbleSet();

        for (byte i = 0; i < NibbleSet.NibbleCount; i++)
        {
            children[i] = true;
        }

        // Start with full RLPMemo.
        InsertRandomKeccak(ref memo, children, out var data, workingSet);

        for (var i = 0; i < numOperations; i++)
        {
            var child = (byte)rand.Next(NibbleSet.NibbleCount);
            var op = (RlpMemoOperation)rand.Next(Enum.GetValues<RlpMemoOperation>().Length);

            switch (op)
            {
                case RlpMemoOperation.Set:
                    if (memo.Exists(child))
                    {
                        data[child] = rand.NextKeccak();
                        memo.Set(data[child].Span, child);

                        memo.TryGetKeccak(child, out var k).Should().BeTrue();
                        k.SequenceEqual(data[child].Span).Should().BeTrue();
                    }

                    break;
                case RlpMemoOperation.Clear:
                    if (memo.Exists(child))
                    {
                        data[child] = Keccak.Zero;
                        memo.Clear(child);

                        memo.TryGetKeccak(child, out var k).Should().BeFalse();
                        k.IsEmpty.Should().BeTrue();
                    }

                    break;
                case RlpMemoOperation.Delete:
                    if (memo.Exists(child))
                    {
                        children[child] = false;
                        data.Remove(child);
                        memo = RlpMemo.Delete(memo, child, workingSet);

                        memo.TryGetKeccak(child, out var k).Should().BeFalse();
                        k.IsEmpty.Should().BeTrue();
                    }

                    break;
                case RlpMemoOperation.Insert:
                    if (!memo.Exists(child))
                    {
                        children[child] = true;
                        data[child] = rand.NextKeccak();
                        memo = RlpMemo.Insert(memo, child, data[child].Span, workingSet);

                        memo.TryGetKeccak(child, out var k).Should().BeTrue();
                        k.SequenceEqual(data[child].Span).Should().BeTrue();
                    }
                    break;
            }

            CompareMemoAndDict(memo, data);
        }
    }

    private static void InsertRandomKeccak(ref RlpMemo memo, NibbleSet.Readonly children, out Dictionary<byte, Keccak> data
        , Span<byte> workingMemory)
    {
        data = new Dictionary<byte, Keccak>();
        var random = new Random(13);

        for (byte i = 0; i < NibbleSet.NibbleCount; i++)
        {
            if (children[i])
            {
                data[i] = random.NextKeccak();
                memo = RlpMemo.Insert(memo, i, data[i].Span, workingMemory);
            }
        }
    }

    private static void CompareMemoAndDict(RlpMemo memo, Dictionary<byte, Keccak> data)
    {
        // All the elements in dictionary should be in the memo.
        foreach (var child in data)
        {
            memo.Exists(child.Key).Should().BeTrue();

            if (child.Value == Keccak.Zero)
            {
                memo.TryGetKeccak(child.Key, out var k).Should().BeFalse();
                k.IsEmpty.Should().BeTrue();
            }
            else
            {
                memo.TryGetKeccak(child.Key, out var k).Should().BeTrue();
                k.SequenceEqual(child.Value.Span).Should().BeTrue();
            }
        }

        // All the elements in the memo should be in the dictionary.
        for (byte i = 0; i < NibbleSet.NibbleCount; i++)
        {
            if (memo.Exists(i))
            {
                data.ContainsKey(i).Should().BeTrue();

                if (memo.TryGetKeccak(i, out var k))
                {
                    k.SequenceEqual(data[i].Span).Should().BeTrue();
                }
                else
                {
                    k.IsEmpty.Should().BeTrue();
                    Keccak.Zero.Span.SequenceEqual(data[i].Span).Should().BeTrue();
                }
            }
        }

        memo.Length.Should().Be(GetExpectedSize(data.Count));
    }

    private static int GetExpectedSize(int numElements)
    {
        var size = numElements * Keccak.Size;

        // Empty and full memo doesn't contain the index.
        if (size != 0 && size != RlpMemo.MaxSize)
        {
            size += NibbleSet.MaxByteSize;
        }

        return size;
    }
}
