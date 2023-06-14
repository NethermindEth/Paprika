using Nethermind.Int256;
using Paprika.Crypto;
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
    bool TryGet(in Key key, out ReadOnlySpan<byte> result);
}
