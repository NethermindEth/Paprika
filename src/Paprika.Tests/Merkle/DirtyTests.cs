using NUnit.Framework;
using Paprika.Chain;
using Paprika.Data;
using Paprika.Merkle;

namespace Paprika.Tests.Merkle;

public class DirtyTests
{
    [Test(Description = "No values set, no changes tracked")]
    public void Empty()
    {
        var merkle = new ComputeMerkleBehavior();
        var commit = new Commit();

        merkle.BeforeCommit(commit);

        commit.StartAssert();

        commit.ShouldBeEmpty();
    }

    [Test(Description = "Single account should create only a single leaf")]
    public void Single_account()
    {
        var path = NibblePath.Parse("012345");
        var account = Key.Account(path);

        var merkle = new ComputeMerkleBehavior();
        var commit = new Commit();

        commit.Set(account, new byte[] { 1 });

        merkle.BeforeCommit(commit);

        commit.StartAssert();
        commit.SetLeaf(Key.Merkle(NibblePath.Empty), path);
        commit.ShouldBeEmpty();
    }

    [Test(Description = "Three accounts, diffing at first nibble. The root is a branch with nibbles set for leafs.")]
    public void Three_accounts_sharing_start_nibble()
    {
        var key0 = NibblePath.Parse("A0000001");
        var key1 = NibblePath.Parse("B0000002");
        var key2 = NibblePath.Parse("C0000003");

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

        commit.SetLeafWithSplitOn(key0, splitOnNibble);
        commit.SetLeafWithSplitOn(key1, splitOnNibble);
        commit.SetLeafWithSplitOn(key2, splitOnNibble);

        commit.SetBranchAllDirty(Key.Merkle(NibblePath.Empty), new NibbleSet(0xA, 0xB, 0xC));

        commit.ShouldBeEmpty();
    }

    [Test(Description =
        "Three accounts, sharing first nibble. The root is an extension -> branch -> with nibbles set for leafs.")]
    public void Three_accounts_starting_with_same_nibble()
    {
        var key0 = NibblePath.Parse("00000001");
        var key1 = NibblePath.Parse("07000002");
        var key2 = NibblePath.Parse("0A000003");

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

        var branchPath = key0.SliceTo(1);
        commit.SetBranchAllDirty(Key.Merkle(branchPath), new NibbleSet(0, 7, 0xA));

        commit.SetLeafWithSplitOn(key0, splitOnNibble);
        commit.SetLeafWithSplitOn(key1, splitOnNibble);
        commit.SetLeafWithSplitOn(key2, splitOnNibble);

        commit.SetExtension(Key.Merkle(NibblePath.Empty), key0.SliceTo(1));

        commit.ShouldBeEmpty();
    }

    [Test]
    public void Long_extension_split_on_first()
    {
        var key0 = NibblePath.Parse("00010001");
        var key1 = NibblePath.Parse("00020002");
        var key2 = NibblePath.Parse("20020003");

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

        commit.SetExtension(Key.Merkle(key0.SliceTo(1)), key0.SliceFrom(1).SliceTo(2));

        // most nested children
        const int branchAt = 3;
        commit.SetBranchAllDirty(Key.Merkle(key0.SliceTo(branchAt)),
            new NibbleSet(key0.GetAt(branchAt), key1.GetAt(branchAt)));
        commit.SetLeafWithSplitOn(key0, branchAt + 1);
        commit.SetLeafWithSplitOn(key1, branchAt + 1);

        commit.SetLeafWithSplitOn(key2, 1);

        commit.ShouldBeEmpty();
    }

    [Test(Description = "Split extension into a branch on the first nibble.")]
    public void Root_extension_split()
    {
        var key0 = NibblePath.Parse("00030001");
        var key1 = NibblePath.Parse("07030002");
        var key2 = NibblePath.Parse("30030003");

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

        commit.SetBranchAllDirty(Key.Merkle(key0.SliceTo(1)), new NibbleSet(0, 7));

        commit.SetLeafWithSplitOn(key0, 2);
        commit.SetLeafWithSplitOn(key1, 2);
        commit.SetLeafWithSplitOn(key2, 1);

        commit.ShouldBeEmpty();
    }

    [Test]
    public void Extension_split_last_nibble()
    {
        var key0 = NibblePath.Parse("00030001");
        var key1 = NibblePath.Parse("00A30002");
        var key2 = NibblePath.Parse("0BA30003");

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

        commit.SetExtension(Key.Merkle(NibblePath.Empty), key0.SliceTo(1));

        commit.SetBranchAllDirty(Key.Merkle(key0.SliceTo(1)), new NibbleSet(0, 0x0B));

        commit.SetBranchAllDirty(Key.Merkle(key0.SliceTo(2)), new NibbleSet(0, 0x0A));

        commit.SetLeafWithSplitOn(key0, 3);
        commit.SetLeafWithSplitOn(key1, 3);
        commit.SetLeafWithSplitOn(key2, 2);

        commit.ShouldBeEmpty();
    }

    [Test]
    public void Extension_split_in_the_middle()
    {
        var key0 = NibblePath.Parse("00000001");
        var key1 = NibblePath.Parse("0000A002");
        var key2 = NibblePath.Parse("00B00003");

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

        commit.SetExtension(Key.Merkle(NibblePath.Empty), key0.SliceTo(2));

        commit.SetBranchAllDirty(Key.Merkle(key2.SliceTo(2)), new NibbleSet(0, 0xB));

        // 0x00B
        commit.SetLeafWithSplitOn(key2, 3);

        // 0x000
        commit.SetExtension(Key.Merkle(key0.SliceTo(3)), key0.SliceFrom(3).SliceTo(1));

        // 0x0000
        const int branchSplitAt = 4;
        commit.SetBranchAllDirty(Key.Merkle(key0.SliceTo(branchSplitAt)), new NibbleSet(0, 0xA));

        // 0x00000
        commit.SetLeafWithSplitOn(key0, branchSplitAt + 1);

        // 0x0000A
        commit.SetLeafWithSplitOn(key1, branchSplitAt + 1);

        commit.ShouldBeEmpty();
    }
}

public static class CommitExtensions
{
    public static void SetLeafWithSplitOn(this ICommit commit, in NibblePath key, int splitOnNibble)
    {
        commit.SetLeaf(Key.Merkle(key.SliceTo(splitOnNibble)), key.SliceFrom(splitOnNibble));
    }
}