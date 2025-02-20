using Paprika.Data;
using Paprika.Store;

namespace Paprika;

public interface IReadOnlyBatch : IDataGetter, IDisposable
{
    public RootPage Root { get; }

    public uint BatchId { get; }

    public void VerifyNoPagesMissing();
}

public interface IDataGetter
{
    Metadata Metadata { get; }

    /// <summary>
    /// Low level retrieval of data.
    /// </summary>
    /// <param name="key"></param>
    /// <param name="result"></param>
    /// <returns></returns>
    bool TryGet(scoped in Key key, out ReadOnlySpan<byte> result);
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

    public RootPage Root => throw new NotImplementedException();

    public uint BatchId => throw new NotImplementedException();

    public void VerifyNoPagesMissing() { }

    public void Dispose() { }
}


