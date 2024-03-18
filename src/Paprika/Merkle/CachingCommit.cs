using System.Collections.Specialized;
using System.Diagnostics;
using Paprika.Chain;
using Paprika.Crypto;
using Paprika.Data;
using Paprika.Store;
using Paprika.Utils;

namespace Paprika.Merkle;

/// <summary>
/// A commit wrapper that provides caching for the upper layers of the Merkle tree up to inclusive <see cref="MaxMerkleTrieLevel"/>.
/// </summary>
public class CachingCommit(ICommit original, BufferPool pool) : ICommit, IDisposable
{
    private const int MaxMerkleTrieLevel = 1;

    /// <summary>
    /// Bytes used per entry.
    /// </summary>
    private const int EntrySize = 128;

    private readonly Page _page = pool.Rent(false);
    private BitVector32 _set;

    public ReadOnlySpanOwnerWithMetadata<byte> Get(scoped in Key key)
    {
        if (ShouldCache(key))
        {
            var id = GetId(key);
            if (_set[id])
            {
                return new ReadOnlySpanOwner<byte>(ReadActual(id), null).WithDepth(0);
            }

            var actual = original.Get(key);
            _set[id] = true;
            var copy = WriteActual(id, actual.Span);

            return new ReadOnlySpanOwner<byte>(copy, null).WithDepth(0);
        }

        return original.Get(key);
    }

    private static bool ShouldCache(in Key key) => key.Type == DataType.Merkle && key.StoragePath.Length == 0 && key.Path.Length <= MaxMerkleTrieLevel;

    public void Set(in Key key, in ReadOnlySpan<byte> payload, EntryType type = EntryType.Persistent)
    {
        if (ShouldCache(key))
        {
            var id = GetId(key);
            _set[id] = true;
            WriteActual(id, payload);
        }

        original.Set(key, payload, type);
    }

    public void Set(in Key key, in ReadOnlySpan<byte> payload0, in ReadOnlySpan<byte> payload1, EntryType type = EntryType.Persistent)
    {
        if (ShouldCache(key))
        {
            var id = GetId(key);
            _set[id] = true;
            WriteActual(id, payload0, payload1);
        }

        original.Set(key, payload0, payload1, type);
    }

    private ReadOnlySpan<byte> ReadActual(int id)
    {
        var span = GetSpan(id);
        return span.Slice(1, span[0]);
    }

    private ReadOnlySpan<byte> WriteActual(int id, ReadOnlySpan<byte> payload)
    {
        var span = GetSpan(id);
        span[0] = (byte)payload.Length;
        var destination = span.Slice(1);
        payload.CopyTo(destination);
        return destination.Slice(1, payload.Length);
    }

    private void WriteActual(int id, ReadOnlySpan<byte> payload0, ReadOnlySpan<byte> payload1)
    {
        var span = GetSpan(id);
        var total = payload0.Length + payload1.Length;

        span[0] = (byte)total;

        var destination = span[1..];
        payload0.CopyTo(destination);
        payload1.CopyTo(destination.Slice(payload0.Length));
    }

    private unsafe Span<byte> GetSpan(int id)
    {
        const int maxId = 17;

        Debug.Assert(EntrySize * maxId < Page.PageSize);

        return new Span<byte>(_page.Payload + id * EntrySize, EntrySize);
    }

    /// <summary>
    /// Gets the id. The root is at 0, 1st level of the tree from 1 to 16 included.
    /// </summary>
    private static int GetId(in Key key) => key.Path.Length == 0 ? 0 : 1 + key.Path.FirstNibble;

    public IChildCommit GetChild() => original.GetChild();

    public IReadOnlyDictionary<Keccak, int> Stats => throw new Exception();

    public void Dispose() => pool.Return(_page);
}