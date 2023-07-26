using System.Text;
using FluentAssertions;
using NUnit.Framework;
using Paprika.Chain;
using Paprika.Crypto;
using Paprika.Data;
using Paprika.Merkle;
using Paprika.Utils;
using static Paprika.Tests.Values;

namespace Paprika.Tests.Merkle;

public class DirtyTests
{
    [Test(Description = "No values set, no changes tracked")]
    public void Empty()
    {
        var merkle = new ComputeMerkleBehavior();
        var commit = new Commit();

        merkle.BeforeCommit(commit);

        commit.ShouldBeEmpty();
    }

    [Test(Description = "Single account should create only a single leaf")]
    public void Single_account()
    {
        var account = Key.Account(Key2);

        var merkle = new ComputeMerkleBehavior();
        var commit = new Commit();

        commit.Set(account, new byte[] { 1 });

        merkle.BeforeCommit(commit);

        commit.StartAssert();
        commit.SetLeaf(Key.Merkle(NibblePath.Empty), NibblePath.FromKey(Key2));
        commit.ShouldBeEmpty();
    }

    [Test(Description = "Three accounts, diffing at first nibble. The root is a branch with nibbles set for leafs.")]
    public void Three_accounts_sharing_start_nibble()
    {
        Keccak key0 = new(new byte[]
            { 0, 1, 2, 3, 4, 5, 6, 7, 0, 1, 2, 3, 4, 5, 6, 7, 0, 1, 2, 3, 4, 5, 6, 7, 0, 1, 2, 3, 4, 5, 6, 7, });

        // ReSharper disable once InlineTemporaryVariable, this is a copy
        var key1 = key0;
        key1.BytesAsSpan[0] = 0x10;

        var key2 = key0;
        key2.BytesAsSpan[0] = 0x20;

        var a0 = Key.Account(key0);
        var a1 = Key.Account(key1);
        var a2 = Key.Account(key2);

        var merkle = new ComputeMerkleBehavior();
        var commit = new Commit();

        commit.Set(a0, new byte[] { 1 });
        commit.Set(a1, new byte[] { 2 });
        commit.Set(a2, new byte[] { 3 });

        merkle.BeforeCommit(commit);

        commit.StartAssert();

        const int splitOnNibble = 1;

        commit.SetLeafWithSplitOn(NibblePath.FromKey(key0), splitOnNibble);
        commit.SetLeafWithSplitOn(NibblePath.FromKey(key1), splitOnNibble);
        commit.SetLeafWithSplitOn(NibblePath.FromKey(key2), splitOnNibble);

        commit.SetBranchAllDirty(Key.Merkle(NibblePath.Empty), new NibbleSet(0, 1, 2));

        commit.ShouldBeEmpty();
    }

    [Test(Description =
        "Three accounts, sharing first nibble. The root is an extension -> branch -> with nibbles set for leafs.")]
    public void Three_accounts_starting_with_same_nibble()
    {
        Keccak key0 = new(new byte[]
            { 0, 1, 2, 3, 4, 5, 6, 7, 0, 1, 2, 3, 4, 5, 6, 7, 0, 1, 2, 3, 4, 5, 6, 7, 0, 1, 2, 3, 4, 5, 6, 7, });

        // ReSharper disable once InlineTemporaryVariable, this is a copy
        var key1 = key0;
        key1.BytesAsSpan[0] = 0x07; // set the next nibble

        var key2 = key0;
        key2.BytesAsSpan[0] = 0x0A; // set the next nibble

        var a0 = Key.Account(key0);
        var a1 = Key.Account(key1);
        var a2 = Key.Account(key2);

        var merkle = new ComputeMerkleBehavior();
        var commit = new Commit();

        commit.Set(a0, new byte[] { 1 });
        commit.Set(a1, new byte[] { 2 });
        commit.Set(a2, new byte[] { 3 });

        merkle.BeforeCommit(commit);

        commit.StartAssert();

        const int splitOnNibble = 2;

        var branchPath = NibblePath.FromKey(key0).SliceTo(1);
        commit.SetBranchAllDirty(Key.Merkle(branchPath), new NibbleSet(0, 7, 0xA));

        commit.SetLeafWithSplitOn(NibblePath.FromKey(key0), splitOnNibble);
        commit.SetLeafWithSplitOn(NibblePath.FromKey(key1), splitOnNibble);
        commit.SetLeafWithSplitOn(NibblePath.FromKey(key2), splitOnNibble);

        commit.SetExtension(Key.Merkle(NibblePath.Empty), NibblePath.FromKey(key0).SliceTo(1));

        commit.ShouldBeEmpty();
    }

    [Test()]
    public void Long_extension_split_on_first()
    {
        Keccak key0 = new(new byte[]
            { 0, 1, 2, 3, 4, 5, 6, 7, 0, 1, 2, 3, 4, 5, 6, 7, 0, 1, 2, 3, 4, 5, 6, 7, 0, 1, 2, 3, 4, 5, 6, 7, });

        var key1 = key0;
        key1.BytesAsSpan[1] = 0x02; // extension with length of 2 as it start on the 1st byte

        var key2 = key0;
        key2.BytesAsSpan[0] = 0x20; // make the branch on the 0th

        var a0 = Key.Account(key0);
        var a1 = Key.Account(key1);
        var a2 = Key.Account(key2);

        var merkle = new ComputeMerkleBehavior();
        var commit = new Commit();

        commit.Set(a0, new byte[] { 1 });
        commit.Set(a1, new byte[] { 2 });
        commit.Set(a2, new byte[] { 3 });

        merkle.BeforeCommit(commit);

        commit.StartAssert();

        var nibbles0Th = new NibbleSet(0, 2);
        commit.SetBranch(Key.Merkle(NibblePath.Empty), nibbles0Th, nibbles0Th);

        var extBase = NibblePath.FromKey(key0);
        commit.SetExtension(Key.Merkle(extBase.SliceTo(1)), extBase.SliceFrom(1).SliceTo(2));

        // most nested children
        var childA = NibblePath.FromKey(key0);
        var childB = NibblePath.FromKey(key1);
        const int branchAt = 3;
        commit.SetBranchAllDirty(Key.Merkle(childA.SliceTo(branchAt)),
            new NibbleSet(childA.GetAt(branchAt), childB.GetAt(branchAt)));
        commit.SetLeafWithSplitOn(childA, branchAt + 1);
        commit.SetLeafWithSplitOn(childB, branchAt + 1);

        commit.SetLeafWithSplitOn(NibblePath.FromKey(key2), 1);

        commit.ShouldBeEmpty();
    }

    [Test(Description = "Split extension into a branch on the first nibble.")]
    public void Root_extension_split()
    {
        Keccak key0 = new(new byte[]
            { 0, 1, 2, 3, 4, 5, 6, 7, 0, 1, 2, 3, 4, 5, 6, 7, 0, 1, 2, 3, 4, 5, 6, 7, 0, 1, 2, 3, 4, 5, 6, 7, });

        // ReSharper disable once InlineTemporaryVariable, this is a copy
        var key1 = key0;
        key1.BytesAsSpan[0] = 0x07; // set the next nibble

        var key2 = key0;
        key2.BytesAsSpan[0] = 0x30; // set the next nibble

        var a0 = Key.Account(key0);
        var a1 = Key.Account(key1);
        var a2 = Key.Account(key2);

        var merkle = new ComputeMerkleBehavior();
        var commit = new Commit();

        commit.Set(a0, new byte[] { 1 });
        commit.Set(a1, new byte[] { 2 });
        commit.Set(a2, new byte[] { 3 });

        merkle.BeforeCommit(commit);

        commit.StartAssert();

        commit.SetBranchAllDirty(Key.Merkle(NibblePath.Empty), new NibbleSet(0, 3));

        commit.SetBranchAllDirty(Key.Merkle(NibblePath.FromKey(key0).SliceTo(1)), new NibbleSet(0, 7));

        commit.SetLeafWithSplitOn(NibblePath.FromKey(key0), 2);
        commit.SetLeafWithSplitOn(NibblePath.FromKey(key1), 2);
        commit.SetLeafWithSplitOn(NibblePath.FromKey(key2), 1);

        commit.ShouldBeEmpty();
    }

    [Test(Description = "Split extension into a branch on the first nibble.")]
    public void Extension_split_last_nibble()
    {
        Keccak key0 = new(new byte[]
            { 0, 1, 2, 3, 4, 5, 6, 7, 0, 1, 2, 3, 4, 5, 6, 7, 0, 1, 2, 3, 4, 5, 6, 7, 0, 1, 2, 3, 4, 5, 6, 7, });

        // ReSharper disable once InlineTemporaryVariable, this is a copy
        var key1 = key0;
        key1.BytesAsSpan[1] = 0xA0; // make it branch on the nibbles[2]

        var key2 = key0;
        key2.BytesAsSpan[0] = 0x0B; // make it branch on the nibbles[1]

        var a0 = Key.Account(key0);
        var a1 = Key.Account(key1);
        var a2 = Key.Account(key2);

        var merkle = new ComputeMerkleBehavior();
        var commit = new Commit();

        commit.Set(a0, new byte[] { 1 });
        commit.Set(a1, new byte[] { 2 });
        commit.Set(a2, new byte[] { 3 });

        merkle.BeforeCommit(commit);

        commit.StartAssert();

        commit.SetExtension(Key.Merkle(NibblePath.Empty), NibblePath.FromKey(key0).SliceTo(1));

        commit.SetBranchAllDirty(Key.Merkle(NibblePath.FromKey(key0).SliceTo(1)), new NibbleSet(0, 0x0B));

        commit.SetBranchAllDirty(Key.Merkle(NibblePath.FromKey(key0).SliceTo(2)), new NibbleSet(0, 0x0A));

        commit.SetLeafWithSplitOn(NibblePath.FromKey(key0), 3);
        commit.SetLeafWithSplitOn(NibblePath.FromKey(key1), 3);
        commit.SetLeafWithSplitOn(NibblePath.FromKey(key2), 2);

        commit.ShouldBeEmpty();
    }

    [Test]
    public void Extension_split_in_the_middle()
    {
        Keccak key0 = Keccak.Zero;

        // ReSharper disable once InlineTemporaryVariable, this is a copy
        var key1 = key0;
        key1.BytesAsSpan[2] = 0x10; // make it branch on the nibbles[4]

        var key2 = key0;
        key2.BytesAsSpan[1] = 0x10; // make it branch on the nibbles[2]

        var a0 = Key.Account(key0);
        var a1 = Key.Account(key1);
        var a2 = Key.Account(key2);

        var merkle = new ComputeMerkleBehavior();
        var commit = new Commit();

        commit.Set(a0, new byte[] { 1 });
        commit.Set(a1, new byte[] { 2 });
        commit.Set(a2, new byte[] { 3 });

        merkle.BeforeCommit(commit);

        commit.StartAssert();


        commit.ShouldBeEmpty();
    }

    class Commit : ICommit
    {
        private readonly Dictionary<byte[], byte[]> _before = new(new BytesEqualityComparer());
        private readonly Dictionary<byte[], byte[]> _after = new(new BytesEqualityComparer());
        private bool _asserting;

        public void Set(in Key key, ReadOnlySpan<byte> value)
        {
            _before[GetKey(key)] = value.ToArray();
        }

        public void ShouldBeEmpty()
        {
            if (_after.Count == 0)
                return;

            var sb = new StringBuilder();
            sb.AppendLine("The commit should be empty, but it contains the following keys: ");
            foreach (var (key, value) in _after)
            {
                Key.ReadFrom(key, out Key k);
                sb.AppendLine($"- {k.ToString()}");
            }

            Assert.Fail(sb.ToString());
        }

        ReadOnlySpanOwner<byte> ICommit.Get(in Key key)
        {
            var k = GetKey(key);
            if (_before.TryGetValue(k, out var value))
            {
                return new ReadOnlySpanOwner<byte>(value, null);
            }

            if (_after.TryGetValue(k, out value))
            {
                return new ReadOnlySpanOwner<byte>(value, null);
            }

            return default;
        }

        private static byte[] GetKey(in Key key) => key.WriteTo(stackalloc byte[key.MaxByteLength]).ToArray();

        void ICommit.Set(in Key key, in ReadOnlySpan<byte> payload)
        {
            var bytes = GetKey(key);
            if (_asserting == false)
            {
                _after[bytes] = payload.ToArray();
            }
            else
            {
                _after.Remove(bytes, out var existing).Should().BeTrue($"key {key.ToString()} should exist");
                payload.SequenceEqual(existing).Should().BeTrue("The value should be equal");
            }
        }

        void ICommit.Visit(CommitAction action)
        {
            foreach (var (k, v) in _before)
            {
                Key.ReadFrom(k, out var key);
                action(key, this);
            }
        }

        /// <summary>
        /// Sets the commit into an asserting mode, where all the sets will be removing and asserting values from it.
        /// </summary>
        public void StartAssert()
        {
            _asserting = true;
        }
    }
}

public static class CommitExtensions
{
    public static void SetLeafWithSplitOn(this ICommit commit, in NibblePath key, int splitOnNibble)
    {
        commit.SetLeaf(Key.Merkle(key.SliceTo(splitOnNibble)), key.SliceFrom(splitOnNibble));
    }
}