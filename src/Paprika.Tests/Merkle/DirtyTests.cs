using System.Buffers.Binary;
using NUnit.Framework;
using Paprika.Chain;
using Paprika.Crypto;
using Paprika.Data;
using Paprika.Merkle;

namespace Paprika.Tests.Merkle;

[TestFixture(true, TestName = "With deletes",
    Description = "These tests write data only to delete them before assertion.")]
[TestFixture(false, TestName = "Asserting Merkle structure")]
public class DirtyTests
{
    private readonly bool _delete;

    public DirtyTests(bool delete)
    {
        _delete = delete;
    }

    [Test(Description = "No values set, no changes tracked")]
    public void Empty()
    {
        var commit = new Commit();

        Assert(commit, _ =>
        {
            /* nothing to remove as this is empty set*/
        });
    }

    [Test(Description = "Single account should create only a single leaf")]
    public void Single_account()
    {
        var commit = new Commit();

        const string path = "012345";

        commit.Set(path);

        Assert(commit, c => c.SetLeaf(Key.Merkle(NibblePath.Empty), path));
    }

    [Test(Description = "Three accounts, diffing at first nibble. The root is a branch with nibbles set for leafs.")]
    public void Three_accounts_sharing_start_nibble()
    {
        var commit = new Commit();

        const string key0 = "A0000001";
        const string key1 = "B0000002";
        const string key2 = "C0000003";

        commit.Set(key0);
        commit.Set(key1);
        commit.Set(key2);

        const int splitOnNibble = 1;

        Assert(commit, c =>
        {
            c.SetLeafWithSplitOn(key0, splitOnNibble);
            c.SetLeafWithSplitOn(key1, splitOnNibble);
            c.SetLeafWithSplitOn(key2, splitOnNibble);

            NibbleSet.Readonly children = new NibbleSet(0xA, 0xB, 0xC);
            c.SetBranch(Key.Merkle(NibblePath.Empty), children);
        });
    }

    [Test(Description =
        "Three accounts, sharing first nibble. The root is an extension -> branch -> with nibbles set for leafs.")]
    public void Three_accounts_starting_with_same_nibble()
    {
        var commit = new Commit();

        const string key0 = "00000001";
        const string key1 = "07000002";
        const string key2 = "0A000003";

        commit.Set(key0);
        commit.Set(key1);
        commit.Set(key2);

        const int splitOnNibble = 2;

        Assert(commit, c =>
        {
            var branchPath = NibblePath.Parse(key0).SliceTo(1);
            NibbleSet.Readonly children = new NibbleSet(0, 7, 0xA);
            c.SetBranch(Key.Merkle(branchPath), children);

            c.SetLeafWithSplitOn(key0, splitOnNibble);
            c.SetLeafWithSplitOn(key1, splitOnNibble);
            c.SetLeafWithSplitOn(key2, splitOnNibble);

            c.SetExtension(Key.Merkle(NibblePath.Empty), branchPath);
        });
    }

    [Test]
    public void Long_extension_split_on_first()
    {
        var commit = new Commit();

        const string key0 = "00010001";
        const string key1 = "00020002";
        const string key2 = "20020003";

        commit.Set(key0);
        commit.Set(key1);
        commit.Set(key2);

        Assert(commit, c =>
        {
            var path0 = NibblePath.Parse(key0);
            var path1 = NibblePath.Parse(key1);

            c.SetBranch(Key.Merkle(NibblePath.Empty), new NibbleSet(0, 2));

            c.SetExtension(Key.Merkle(path0.SliceTo(1)), path0.SliceFrom(1).SliceTo(2));

            // most nested children
            const int branchAt = 3;
            NibbleSet.Readonly children = new NibbleSet(path0.GetAt(branchAt), path1.GetAt(branchAt));
            c.SetBranch(Key.Merkle(path0.SliceTo(branchAt)), children);
            c.SetLeafWithSplitOn(path0, branchAt + 1);
            c.SetLeafWithSplitOn(path1, branchAt + 1);

            c.SetLeafWithSplitOn(key2, 1);
        });
    }

    [Test(Description = "Split extension into a branch on the first nibble.")]
    public void Root_extension_split()
    {
        var commit = new Commit();

        const string key0 = "00030001";
        const string key1 = "07030002";
        const string key2 = "30030003";

        commit.Set(key0);
        commit.Set(key1);
        commit.Set(key2);

        Assert(commit, c =>
        {
            var path0 = NibblePath.Parse(key0);

            c.SetBranch(Key.Merkle(NibblePath.Empty), new NibbleSet(0, 3));

            c.SetBranch(Key.Merkle(path0.SliceTo(1)), new NibbleSet(0, 7));

            c.SetLeafWithSplitOn(key0, 2);
            c.SetLeafWithSplitOn(key1, 2);
            c.SetLeafWithSplitOn(key2, 1);
        });
    }

    [Test]
    public void Extension_split_last_nibble()
    {
        var commit = new Commit();

        const string key0 = "00030001";
        const string key1 = "00A30002";
        const string key2 = "0BA30003";

        commit.Set(key0);
        commit.Set(key1);
        commit.Set(key2);

        Assert(commit, c =>
        {
            var path0 = NibblePath.Parse(key0);
            c.SetExtension(Key.Merkle(NibblePath.Empty), path0.SliceTo(1));

            c.SetBranch(Key.Merkle(path0.SliceTo(1)), new NibbleSet(0, 0x0B));

            c.SetBranch(Key.Merkle(path0.SliceTo(2)), new NibbleSet(0, 0x0A));

            c.SetLeafWithSplitOn(path0, 3);
            c.SetLeafWithSplitOn(key1, 3);
            c.SetLeafWithSplitOn(key2, 2);
        });
    }

    [Test]
    public void Extension_split_in_the_middle()
    {
        var commit = new Commit();

        const string key0 = "00000001";
        const string key1 = "0000A002";
        const string key2 = "00B00003";

        commit.Set(key0);
        commit.Set(key1);
        commit.Set(key2);

        Assert(commit, c =>
        {
            var path0 = NibblePath.Parse(key0);
            var path2 = NibblePath.Parse(key2);

            c.SetExtension(Key.Merkle(NibblePath.Empty), path0.SliceTo(2));

            c.SetBranch(Key.Merkle(path2.SliceTo(2)), new NibbleSet(0, 0xB));

            // 0x00B
            c.SetLeafWithSplitOn(path2, 3);

            // 0x000
            c.SetExtension(Key.Merkle(path0.SliceTo(3)), path0.SliceFrom(3).SliceTo(1));

            // 0x0000
            const int branchSplitAt = 4;
            c.SetBranch(Key.Merkle(path0.SliceTo(branchSplitAt)), new NibbleSet(0, 0xA));

            // 0x00000
            c.SetLeafWithSplitOn(path0, branchSplitAt + 1);

            // 0x0000A
            c.SetLeafWithSplitOn(key1, branchSplitAt + 1);
        });
    }

    [Test]
    public void Branch_with_extension_child_even()
    {
        var commit = new Commit();

        const string key0 = "A0010001";
        const string key1 = "B0020002";
        const string key2 = "C0030003";
        const string key3 = "C0040003";

        commit.Set(key0);
        commit.Set(key1);
        commit.Set(key2);
        commit.Set(key3);

        Assert(commit, c =>
        {
            if (_delete == false)
            {
                NUnit.Framework.Assert.Ignore("No asserting for non-delete scenario");
            }
        });
    }

    [Test]
    public void Branch_with_extension_child_odd()
    {
        var commit = new Commit();

        const string key0 = "A0001001";
        const string key1 = "B0002002";
        const string key2 = "C0003003";
        const string key3 = "C0004003";

        commit.Set(key0);
        commit.Set(key1);
        commit.Set(key2);
        commit.Set(key3);

        Assert(commit, c =>
        {
            if (_delete == false)
            {
                NUnit.Framework.Assert.Ignore("No asserting for non-delete scenario");
            }
        });
    }

    [Test]
    public void Big_random_set_and_delete()
    {
        const int size = 10000;
        const int seed = 19;
        var commit = new Commit();

        var random = new Random(seed);

        for (var i = 0; i < size; i++)
        {
            commit.Set(NibblePath.FromKey(random.NextKeccak()));
        }

        Assert(commit, c =>
        {
            if (_delete == false)
            {
                NUnit.Framework.Assert.Ignore("No asserting for non-delete scenario");
            }
        });
    }

    private void Assert(Commit commit, Action<ICommit> assert)
    {
        const int dontMemoize = int.MaxValue;
        var merkle = new ComputeMerkleBehavior(dontMemoize, dontMemoize, Memoization.None);

        // run merkle before
        merkle.BeforeCommit(commit, CacheBudget.Options.None.Build());

        if (_delete)
        {
            // get all the keys inserted before merkle
            var keys = commit.GetSnapshotOfBefore();

            // squash commit to the history
            commit = commit.Squash(true);

            // delete all the keys that were set initially to get clean slate
            foreach (var key in keys)
            {
                commit.DeleteKey(key);
            }

            // run Merkle it again to undo the structure
            merkle.BeforeCommit(commit, CacheBudget.Options.None.Build());
        }

        if (!_delete)
        {
            commit.StartAssert();
            assert(commit);
            commit.ShouldBeEmpty();
        }
        else
        {
            // delete should have everything removed
            commit = commit.Squash(true);
            commit.ShouldHaveSquashedStateEmpty();
        }
    }
}

public static class CommitExtensions
{
    public static void SetLeafWithSplitOn(this ICommit commit, in NibblePath key, int splitOnNibble)
    {
        commit.SetLeaf(Key.Merkle(key.SliceTo(splitOnNibble)), key.SliceFrom(splitOnNibble));
    }

    public static void SetLeafWithSplitOn(this ICommit commit, string path, int splitOnNibble)
    {
        var key = NibblePath.Parse(path);
        commit.SetLeaf(Key.Merkle(key.SliceTo(splitOnNibble)), key.SliceFrom(splitOnNibble));
    }


    public static void SetLeaf(this ICommit commit, in Key key, string leafPath)
    {
        var leaf = new Node.Leaf(NibblePath.Parse(leafPath));
        commit.Set(key, leaf.WriteTo(stackalloc byte[leaf.MaxByteLength]));
    }

    public static void Set(this Commit commit, string path) => commit.Set(NibblePath.Parse(path));

    public static void Set(this Commit commit, in NibblePath path) => commit.Set(Key.Account(path), SmallestAccount);

    private static readonly byte[] SmallestAccount;

    static CommitExtensions()
    {
        var account = new Account(1, 1);
        var written = account.WriteTo(stackalloc byte[Account.MaxByteCount]);
        SmallestAccount = written.ToArray();
    }
}