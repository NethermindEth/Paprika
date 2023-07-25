using Paprika.Chain;
using Paprika.Data;

namespace Paprika.Merkle;

public class ComputeMerkleBehavior : IPreCommitBehavior
{
    public void BeforeCommit(ICommit commit)
    {
        // run the visitor on the commit
        commit.Visit(OnKey);


        // ComputeKeccak("");

        // Recompute the Keccak of all Merkle nodes
        // The root Merkle node should exist on the Empty Path (''), and it's Keccak is the Merkle Root Hash
    }

    private static void OnKey(in Key key, ICommit commit)
    {
        if (key.Type == DataType.Account)
        {
            MarkAccountPathDirty(in key.Path, commit);
        }
        else
        {
            throw new Exception("Not implemented for other types now.");
        }
    }

    private static void MarkAccountPathDirty(in NibblePath path, ICommit commit)
    {
        // Tree evolution, respectively for inserts, if your insert KEY and it
        // 1. meets an extension:
        //      a. and they differ at the 0th nibble:
        //          1. branch on the 0th nibble of extension
        //          2. add leaf with KEY with trimmed start by 1 nibble
        //      b. and they differ in the middle or in the end
        //          1. trim extension till before they differ
        //          2. insert the branch
        //          3. put two leaves in the branch
        // 2. meets a branch:
        //      a. follow the branch, adding the children if needed
        // 3. meets a leaf:
        //      a. KEY and leaf differ at the 0th nibble:
        //          1. branch on the 0th nibble of the leaf
        //          2. add leaf with KEY with trimmed start by 1 nibble
        //      b. differ in the middle or the end:
        //          1. create an extension from the prefix
        //          2. create the branch on the nibble
        //          3. put two leaves in there

        Span<byte> span = stackalloc byte[33];

        for (int i = 0; i < path.Length; i++)
        {
            var slice = path.SliceTo(i);
            var key = Key.Merkle(slice);

            var leftoverPath = path.SliceFrom(i);

            using var owner = commit.Get(key);

            if (owner.IsEmpty)
            {
                // no value set now, create one
                commit.SetLeaf(key, leftoverPath);
                return;
            }

            // read the existing one
            Node.ReadFrom(owner.Span, out var type, out var leaf, out var ext, out var branch);
            switch (type)
            {
                case Node.Type.Leaf:
                    {
                        var diffAt = leaf.Path.FindFirstDifferentNibble(leftoverPath);

                        if (diffAt == leaf.Path.Length)
                        {
                            // update in place, mark in parent as dirty, beside that, do from from the Merkle pov
                            return;
                        }

                        if (diffAt > 0)
                        {
                            // diff is not on the 0th position, so it will be a branch but preceded with an extension
                            commit.SetExtension(key, leftoverPath.SliceTo(diffAt));
                        }

                        var nibbleA = leaf.Path.GetAt(diffAt);
                        var nibbleB = leftoverPath.GetAt(diffAt);

                        // create branch, truncate both leaves, add them at the end
                        var branchKey = Key.Merkle(path.SliceTo(i + diffAt));
                        commit.SetBranch(branchKey,
                            new NibbleSet(nibbleA, nibbleB),
                            new NibbleSet(nibbleA, nibbleB));

                        // nibbleA
                        var written = path.SliceTo(i + 1 + diffAt).WriteTo(span);
                        NibblePath.ReadFrom(written, out var pathA);
                        pathA.UnsafeSetAt(i + diffAt, 0, nibbleA);

                        commit.SetLeaf(Key.Merkle(pathA), leaf.Path.SliceFrom(diffAt + 1));

                        // nibbleB, set the newly set leaf, slice to the next nibble
                        var pathB = path.SliceTo(i + 1 + diffAt);
                        commit.SetLeaf(Key.Merkle(pathB), leftoverPath.SliceFrom(diffAt + 1));

                        return;
                    }
                case Node.Type.Extension:
                    {
                        var diffAt = ext.Path.FindFirstDifferentNibble(leftoverPath);
                        if (diffAt == ext.Path.Length)
                        {
                            // the path overlaps with what is there, move forward
                            i += ext.Path.Length - 1;
                            continue;
                        }

                        if (diffAt == 0)
                        {
                            if (ext.Path.Length == 1)
                            {
                                // before E->B
                                // after  B->B
                                var set = new NibbleSet(ext.Path.GetAt(0), leftoverPath.GetAt(0));
                                commit.SetBranch(key, set, set);
                                commit.SetLeaf(Key.Merkle(path.SliceTo(i + 1)), path.SliceFrom(i + 1));

                                return;
                            }

                            throw new NotImplementedException("Truncate E by 1 from the start.");
                        }

                        throw new NotImplementedException("Other cases");
                    }
                    break;
                case Node.Type.Branch:
                    {
                        var nibble = path.GetAt(i);
                        if (branch.HasKeccak)
                        {
                            // branch has keccak, this means it was not written yet, needs to be dirtied
                            commit.SetBranch(key, branch.Children.Set(nibble), new NibbleSet(nibble));
                        }
                        else
                        {
                            if (branch.Children[nibble] && branch.Dirty[nibble])
                            {
                                // everything set as needed, continue
                                continue;
                            }

                            commit.SetBranch(key, branch.Children.Set(nibble), branch.Dirty.Set(nibble));
                        }
                    }
                    break;
                default:
                    throw new ArgumentOutOfRangeException();
            }
        }
    }
}

public static class CommitExtensions
{
    public static void SetLeaf(this ICommit commit, in Key key, in NibblePath leafPath)
    {
        var leaf = new Node.Leaf(leafPath);
        commit.Set(key, leaf.WriteTo(stackalloc byte[leaf.MaxByteLength]));
    }

    public static void SetBranch(this ICommit commit, in Key key, NibbleSet.Readonly children,
        NibbleSet.Readonly dirtyChildren)
    {
        var branch = new Node.Branch(children, dirtyChildren);
        commit.Set(key, branch.WriteTo(stackalloc byte[branch.MaxByteLength]));
    }

    public static void SetExtension(this ICommit commit, in Key key, in NibblePath path)
    {
        var extension = new Node.Extension(path);
        commit.Set(key, extension.WriteTo(stackalloc byte[extension.MaxByteLength]));
    }
}