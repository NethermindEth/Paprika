using Paprika.Data;

namespace Paprika.Store;

/// <summary>
/// Represents a context for setting the data.
/// </summary>
public readonly ref struct SetContext
{
    public readonly Key Key;
    public readonly IBatchContext Batch;
    public readonly ReadOnlySpan<byte> Data;
    public readonly bool IsPrefixed;

    public SetContext(Key key, ReadOnlySpan<byte> data, IBatchContext batch, bool isPrefixed = false)
    {
        Key = key;
        Batch = batch;
        Data = data;
        IsPrefixed = isPrefixed;
    }

    /// <summary>
    /// Creates the set context with the <see cref="Key"/> sliced from the given nibble.
    /// </summary>
    public SetContext SliceFrom(int nibbleCount) => new(Key.SliceFrom(nibbleCount), Data, Batch);
}