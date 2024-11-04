using System.Buffers;
using Paprika.Crypto;
using Paprika.Data;
using Paprika.Utils;

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
/// Allows <see cref="IPreCommitBehavior"/> prefetches to access ancestors data.
/// </summary>
public interface IPrefetcherContext
{
    bool CanPrefetchFurther { get; }

    /// <summary>
    /// Tries to retrieve the result stored under the given key.
    /// If it fails to get it from the current state, it will fetch it from the ancestors and store it accordingly to the
    /// <paramref name="entryMapping"/>.
    /// </summary>
    public ReadOnlySpanOwner<byte> Get(scoped in Key key, SpanFunc<EntryType> entryMapping);
}

public delegate TResult SpanFunc<TResult>(in ReadOnlySpan<byte> data);
