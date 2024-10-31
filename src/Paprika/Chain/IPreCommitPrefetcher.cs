using Paprika.Crypto;
using Paprika.Data;

namespace Paprika.Chain;

/// <summary>
/// The prefetcher that can be used to prefetch data for <see cref="IPreCommitBehavior"/>
/// to help it with a faster commitment when <see cref="IWorldState.Commit"/> happens.
/// </summary>
public interface IPreCommitPrefetcher
{
    /// <summary>
    /// Whether this prefetcher is still capable of prefetching data.
    /// </summary>
    bool CanPrefetchFurther { get; }

    /// <summary>
    /// Prefetches data needed for the account.
    /// </summary>
    /// <param name="account">The account to be prefetched.</param>
    void PrefetchAccount(in Keccak account);

    /// <summary>
    /// Prefetches data needed for the account.
    /// </summary>
    /// <param name="account">The account to be prefetched.</param>
    /// <param name="storage">The storage slot</param>
    void PrefetchStorage(in Keccak account, in Keccak storage);
}

/// <summary>
/// Allows <see cref="IPreCommitBehavior.Prefetch"/> to access ancestors data.
/// </summary>
public interface IPrefetcherContext
{
    bool CanPrefetchFurther { get; }

    /// <summary>
    /// Tries to retrieve the result stored under the given key.
    /// </summary>
    /// <remarks>
    /// Returns a result as an owner that must be disposed properly (using var owner = Get)
    /// </remarks>
    public ReadOnlySpanOwnerWithMetadata<byte> Get(scoped in Key key);

    /// <summary>
    /// Sets the value under the given key.
    /// </summary>
    bool Set(in Key key, in ReadOnlySpan<byte> payload, EntryType type = EntryType.Persistent);
}