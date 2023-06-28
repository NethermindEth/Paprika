using Paprika.Chain;

namespace Paprika.Merkle;

public class ComputeMerkleBehavior : IPreCommitBehavior
{
    public void BeforeCommit(ICommit commit)
    {
        // Foreach key in the commit:
        //      > Set each intermediate Merkle node as 'Dirty'
        // Modify any intermediate Merkle node if there were any inserts (7 cases)
        // Recompute the Keccak of all Merkle nodes
        // The root Merkle node should exist on the Empty Path (''), and it's Keccak is the Merkle Root Hash
    }
}
