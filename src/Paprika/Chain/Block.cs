using Paprika.Crypto;

namespace Paprika.Chain;

/// <summary>
/// Represents a block that is a result of ExecutionPayload.
/// It's capable of storing uncommitted data in its internal <see cref="LinkedMap"/>.  
/// </summary>
public class Block : IDisposable
{
    public Keccak ParentHash { get; }
    public int BlockNumber { get; }
    
    private readonly LinkedMap _map;

    public Block(Keccak parentHash, int blockNumber)
    {
        ParentHash = parentHash;
        BlockNumber = blockNumber;
        _map = new LinkedMap();
    }

    public void Dispose() => _map.Dispose();
}