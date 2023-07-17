using Paprika.Chain;
using Paprika.Data;

namespace Paprika.Merkle;

public class ComputeMerkleBehavior : IPreCommitBehavior
{
    public void BeforeCommit(ICommit commit)
    {
        // Foreach key in the commit:
        //      > Set each intermediate Merkle node as 'Dirty'
        // Modify any intermediate Merkle node if there were any inserts (7 cases)
        commit.Visit(OnKey);


        // ComputeKeccak("");

        // Recompute the Keccak of all Merkle nodes
        // The root Merkle node should exist on the Empty Path (''), and it's Keccak is the Merkle Root Hash
    }

    private void OnKey(in Key key, ICommit commit)
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

            var owner = commit.Get(key);

            var notExist = owner.Span.IsEmpty;
            if (notExist)
            {
                // no value at the node, set leaf
            }
            else
            {
                // there's value, make it
            }

        }
    }
}