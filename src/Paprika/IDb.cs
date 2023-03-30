using System.Runtime.CompilerServices;
using Nethermind.Int256;
using Paprika.Crypto;

namespace Paprika;

public interface IDb
{
    ITransaction Begin();
}

public interface ITransaction : IDisposable
{
    bool TryGetNonce(in Keccak key, out UInt256 nonce);

    void Set(in Keccak key, in UInt256 balance, in UInt256 nonce);

    void Commit(CommitOptions options);

    double TotalUsedPages { get; }
}

public enum CommitOptions
{
    /// <summary>
    /// Flushes db only once, ensuring that the data are stored properly.
    /// The root is stored ephemerally, waiting for the next commit to be truly stored.
    ///
    /// This guarantees ATOMIC (from ACID) but not DURABLE.  
    /// </summary>
    FlushDataOnly,

    /// <summary>
    /// Flush data, then the root.
    ///
    /// This guarantees ATOMIC and DURABLE (from ACID).
    /// </summary>
    FlushDataAndRoot
}