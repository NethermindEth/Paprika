using Paprika.Crypto;
using Paprika.Data;

namespace Paprika.Chain;

/// <summary>
/// The prefetcher that can be used to prefetch data for <see cref="IPreCommitBehavior"/>
/// to help it with a faster commitment when <see cref="IWorldState.Commit"/> happens.
/// </summary>
public interface IPreCommitPrefetcher<TAccount, TStorage>
{
    /// <summary>
    /// Whether this prefetcher is still capable of prefetching data.
    /// </summary>
    bool CanPrefetchFurther { get; }

    /// <summary>
    /// Prefetches data needed for the account.
    /// </summary>
    /// <param name="account">The account to be prefetched.</param>
    void PrefetchAccount(in TAccount account);

    /// <summary>
    /// Prefetches data needed for the account.
    /// </summary>
    /// <param name="account">The account to be prefetched.</param>
    /// <param name="storage">The storage slot</param>
    void PrefetchStorage(in TAccount account, in TStorage storage);
}

public interface IPrefetchMapping<TAccount, TStorage>
{
    public static abstract int GetHashCode(in TAccount account);
    public static abstract int GetStorageHashCode(in TStorage storage);

    public static abstract Keccak ToKeccak(in TAccount account);
    public static abstract Keccak ToStorageKeccak(in TStorage storage);
}

public struct IdentityPrefetchMapping : IPrefetchMapping<Keccak, Keccak>
{
    public static int GetHashCode(in Keccak storage) => storage.GetHashCode();
    public static int GetStorageHashCode(in Keccak account) => account.GetHashCode();

    public static Keccak ToKeccak(in Keccak account) => account;

    public static Keccak ToStorageKeccak(in Keccak storage) => storage;
}

/// <summary>
/// Allows <see cref="IPreCommitBehavior.Prefetch"/> to access ancestors data.
/// </summary>
public interface IPrefetcherContext
{
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
    /// <remarks>Whether set was successful. If false, the consecutive sets will fail as well.</remarks>
    bool TrySet(in Key key, in ReadOnlySpan<byte> payload, EntryType type = EntryType.Persistent);
}