using System.Buffers;
using Paprika.Data;

namespace Paprika.Chain;

/// <summary>
/// Represents a map based on <see cref="FixedMap"/> that has a capability of growing and linking its chains.
/// </summary>
/// <remarks>
/// Slapped all the functionalities together to make it as small as possible.
/// </remarks>
public class LinkedMap : IDisposable
{
    private const int Size = 32 * 1024;
    
    // TODO: a custom pool?
    private static readonly ArrayPool<byte> Pool = ArrayPool<byte>.Shared;

    private readonly byte[] _bytes;
    private readonly LinkedMap? _next;

    public LinkedMap() : this(null)
    {
    }

    private LinkedMap(LinkedMap? next)
    {
        _bytes = Pool.Rent(Size);
        _bytes.AsSpan().Clear();

        _next = next;
    }

    /// <summary>
    /// Sets the value, returning the actual linked map that should be memoized.
    /// </summary>
    /// <param name="key"></param>
    /// <param name="data"></param>
    /// <returns></returns>
    public LinkedMap Set(Key key, ReadOnlySpan<byte> data)
    {
        if (Map.TrySet(key, data))
        {
            return this;
        }

        var map = new LinkedMap(this);
        return map.Set(key, data);
    }

    /// <summary>
    /// Tries to retrieve a value from a linked map.
    /// </summary>
    public bool TryGet(in Key key, out ReadOnlySpan<byte> result)
    {
        if (Map.TryGet(key, out result))
        {
            return true;
        }

        if (_next != null)
            return _next.TryGet(in key, out result);

        return false;
    }

    private FixedMap Map => new(_bytes);

    public void Dispose()
    {
        Pool.Return(_bytes);
        _next?.Dispose();
    }
}