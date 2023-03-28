using System.Runtime.CompilerServices;

namespace Paprika;

public interface IDb
{
    ITransaction Begin();
}

public interface ITransaction
{
    bool TryGet(in ReadOnlySpan<byte> key, out ReadOnlySpan<byte> value);

    void Set(in ReadOnlySpan<byte> key, in ReadOnlySpan<byte> value);

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