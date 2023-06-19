using Paprika.Chain;

namespace Paprika.Merkle;

public class RecomputeMerkle : IPreCommitBehavior
{
    public void BeforeCommit(ICommit commit)
    {
        foreach (var key in commit)
        {
            // Mark each intermediate Merkle node for each key as 'Dirty'
        }
        // Modify the tree structure if there were any inserts (7 cases)
        // Recompute all Keccaks in intermediate nodes
    }
}
