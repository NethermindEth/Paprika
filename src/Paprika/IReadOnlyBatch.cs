using Paprika.Data;
using Paprika.Store;

namespace Paprika;

public interface IReadOnlyBatch : IDisposable
{
    Metadata Metadata { get; }

    /// <summary>
    /// Low level retrieval of data.
    /// </summary>
    /// <param name="key"></param>
    /// <param name="result"></param>
    /// <returns></returns>
    bool TryGet(scoped in Key key, out ReadOnlySpan<byte> result);

    void Report(IReporter reporter);

    public uint BatchId { get; }
}

public class EmptyReadOnlyBatch : IReadOnlyBatch
{
    public static readonly EmptyReadOnlyBatch Instance = new();

    public Metadata Metadata => default;

    /// <summary>
    /// Low level retrieval of data.
    /// </summary>
    /// <param name="key"></param>
    /// <param name="result"></param>
    /// <returns></returns>
    public bool TryGet(scoped in Key key, out ReadOnlySpan<byte> result)
    {
        result = default;
        return false;
    }

    public void Report(IReporter reporter) { }

    public uint BatchId => default;
    public void Dispose() { }
}


