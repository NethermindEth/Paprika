using Paprika.Data;

namespace Paprika.Store;

/// <summary>
/// Represents a context for setting the data.
/// </summary>
public readonly ref struct SetContext
{
    public readonly uint Hash;
    public readonly Key Key;
    public readonly IBatchContext Batch;
    public readonly ReadOnlySpan<byte> Data;

    public SetContext(uint hash, Key key, ReadOnlySpan<byte> data, IBatchContext batch)
    {
        Hash = hash;
        Key = key;
        Batch = batch;
        Data = data;
    }

    /// <summary>
    /// Creates the set context with the <see cref="Key"/> sliced from the given nibble.
    /// </summary>
    public SetContext SliceFrom(int nibbleCount) => new(Hash, Key.SliceFrom(nibbleCount), Data, Batch);
}