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

        for (int i = 0; i < path.Length; i++)
        {
            var slice = path.SliceTo(i);
            var key = Key.Merkle(slice);

            var leftoverPath = path.SliceFrom(i);

            using var owner = commit.Get(key);

            if (owner.IsEmpty)
            {
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
                            // update in place, nothing to do from from the Merkle pov
                        }
                        else if (diffAt == 0)
                        {
                            // create branch, truncate both leaves, add them at the end
                        }
                        else
                        {
                            // create extension->branch-> leaves
                        }
                        break;
                    }
                case Node.Type.Extension:
                    {
                        var diffAt = ext.Path.FindFirstDifferentNibble(leftoverPath);
                        if (diffAt == 0)
                        {
                            if (ext.Path.Length == 1)
                            {
                                // before E->B
                                // after  B->B
                                // TODO: create branch instead of ext
                                // put the ext.branch underneath
                                // put leaf underneath

                            }

                            // if extension would be empty, follow with the next branch + leaf
                            // if extension truncate both leaves, add them at the end
                        }
                        else
                        {
                            // create extension->branch-> leaves
                        }

                    }
                    break;
                    break;
                case Node.Type.Branch:
                    break;
                default:
                    throw new ArgumentOutOfRangeException();
            }
            // there's value, make it
        }
    }
}

file static class CommitExtensions
{
    public static void SetLeaf(this ICommit commit, in Key key, in NibblePath leafPath)
    {
        var leaf = new Node.Leaf(leafPath);
        commit.Set(key, leaf.WriteTo(stackalloc byte[leaf.MaxByteLength]));
    }
}