using Paprika.Chain;
using Paprika.Crypto;

namespace Paprika.Merkle;

public static class WorldStateExtensions
{
    public static Keccak GetMerkleRootHash(this IWorldState state)
    {
        return Keccak.EmptyTreeHash;
    }
}
