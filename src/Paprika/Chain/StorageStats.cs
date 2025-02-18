using Paprika.Crypto;

namespace Paprika.Chain;

public sealed class StorageStats : IStorageStats
{
    private readonly HashSet<Keccak> _set = new();
    private readonly HashSet<Keccak> _deleted = new();

    public IReadOnlySet<Keccak> Set => _set;
    public IReadOnlySet<Keccak> Deleted => _deleted;

    /// <summary>
    /// Reacts to the <paramref name="value"/> set for the given <paramref name="storage"/> cell.
    /// </summary>
    public void SetStorage(in Keccak storage, in ReadOnlySpan<byte> value)
    {
        if (value.IsEmpty)
        {
            if (_deleted.Add(storage))
            {
                // Optimization. Previously was not deleted, might have been set
                _set.Remove(storage);
            }
        }
        else
        {
            // Non-empty value
            if (_set.Add(storage))
            {
                // Optimization. Previously was not set, might have been deleted
                _deleted.Remove(storage);
            }
        }
    }
}

/// <summary>
/// Represents sets of keys that were <see cref="Set"/> and <see cref="Deleted"/> during <see cref="ICommitWithStats"/>.
/// </summary>
public interface IStorageStats
{
    /// <summary>
    /// Keys that were set.
    /// </summary>
    public IReadOnlySet<Keccak> Set { get; }

    /// <summary>
    /// Keys that were deleted.
    /// </summary>
    public IReadOnlySet<Keccak> Deleted { get; }
}