using Paprika.Crypto;

namespace Paprika;

public interface IDb
{
    /// <summary>
    /// Starts a db transaction that is for the next block.
    /// </summary>
    /// <returns>The transaction that handles block operations.</returns>
    IBatch BeginNextBlock();

    /// <summary>
    /// Reorganizes chain back to the given block hash and starts building on top of it.
    /// </summary>
    /// <param name="stateRootHash">The block hash to reorganize to.</param>
    /// <returns>The new batch.</returns>
    IBatch ReorganizeBackToAndStartNew(Keccak stateRootHash);
}