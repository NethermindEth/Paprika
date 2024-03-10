using Paprika.Crypto;
using Paprika.Data;
using Paprika.Merkle;
using Paprika.Utils;

namespace Paprika.Chain;

/// <summary>
/// A pre-commit behavior run by <see cref="Blockchain"/> component just before commiting a <see cref="IWorldState"/>
/// with <see cref="IWorldState.Commit"/>. Useful to provide concerns, like the Merkle construct and others.
/// </summary>
public interface IPreCommitBehavior
{
    /// <summary>
    /// Executed just before commit.
    /// </summary>
    /// <param name="commit">The object representing the commit.</param>
    /// <param name="budget">The budget for caching capabilities.</param>
    /// <returns>The result of the before commit.</returns>
    Keccak BeforeCommit(ICommit commit, CacheBudget budget);

    /// <summary>
    /// Inspects the data allowing it to overwrite them if needed, before the commit is applied to the database.
    /// </summary>
    /// <param name="key">The key related to data.</param>
    /// <param name="data">The data.</param>
    /// <returns>The data that should be put in place.</returns>
    ReadOnlySpan<byte> InspectBeforeApply(in Key key, ReadOnlySpan<byte> data) => data;

    /// <summary>
    /// Executed when the new account is created, allowing for some optimizations.
    /// </summary>
    void OnNewAccountCreated(in Keccak address, ICommit commit)
    {
    }

    /// <summary>
    /// Executed after the current state & storage is cleared.
    /// </summary>
    void OnAccountDestroyed(in Keccak address, ICommit commit)
    {
    }
}

/// <summary>
/// Provides the set of changes applied onto <see cref="IWorldState"/>,
/// allowing for additional modifications of the data just before the commit.
/// </summary>
/// <remarks>
/// Use <see cref="Visit"/> to access all the keys.
/// </remarks>
public interface ICommit
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
    void Set(in Key key, in ReadOnlySpan<byte> payload, EntryType type = EntryType.Persistent);

    /// <summary>
    /// Sets the value under the given key.
    /// </summary>
    void Set(in Key key, in ReadOnlySpan<byte> payload0, in ReadOnlySpan<byte> payload1, EntryType type = EntryType.Persistent);

    /// <summary>
    /// Visits the given <paramref name="type"/> of the changes in the given commit.
    /// </summary>
    void Visit(CommitAction action, TrieType type) => throw new Exception("No visitor available for this commit");

    /// <summary>
    /// Gets the child commit that is a thread-safe write-through commit.
    /// </summary>
    /// <returns>A child commit.</returns>
    IChildCommit GetChild();

    /// <summary>
    /// Gets the statistics of sets in the given commit.
    /// Account writes make just key appear.
    /// Storage writes increase it by 1.
    /// </summary>
    IReadOnlyDictionary<Keccak, int> Stats { get; }
}

public interface IReadOnlyCommit
{
    /// <summary>
    /// Tries to retrieve the result stored under the given key only from this commit.
    /// </summary>
    /// <remarks>
    /// If successful, returns a result as an owner. Must be disposed properly.
    /// </remarks>
    public ReadOnlySpanOwnerWithMetadata<byte> Get(scoped in Key key);
}

public interface IChildCommit : ICommit, IDisposable
{
    /// <summary>
    /// Commits to the parent
    /// </summary>
    void Commit();
}

/// <summary>
/// A delegate to be called on the each key that that the commit contains.
/// </summary>
public delegate void CommitAction(in Key key, ReadOnlySpan<byte> value);

/// <summary>
/// Provides the same capability as <see cref="ReadOnlySpanOwner{T}"/> but with the additional metadata.
/// </summary>
/// <typeparam name="T"></typeparam>
public readonly ref struct ReadOnlySpanOwnerWithMetadata<T>(ReadOnlySpanOwner<T> owner, ushort queryDepth)
{
    public const ushort DatabaseQueryDepth = ushort.MaxValue;

    /// <summary>
    /// Provides the depths of the query to retrieve the value.
    ///
    /// 0 is for the current commit.
    /// 1-N is for blocks
    /// <see cref="DatabaseQueryDepth"/> is for a query hitting the db transaction. 
    /// </summary>
    public ushort QueryDepth { get; } = queryDepth;

    private readonly ReadOnlySpanOwner<T> _owner = owner;

    public ReadOnlySpan<T> Span => _owner.Span;

    public bool IsEmpty => _owner.IsEmpty;

    /// <summary>
    /// Disposes the owner provided as <see cref="IDisposable"/> once.
    /// </summary>
    public void Dispose() => _owner.Dispose();

    /// <summary>
    /// Answers whether this span is owned and provided by <paramref name="owner"/>.
    /// </summary>
    public bool IsOwnedBy(object owner) => _owner.IsOwnedBy(owner);

    /// <summary>
    /// Increases the <see cref="QueryDepth"/> of this span owner, reporting it as more nested.
    /// </summary>
    public ReadOnlySpanOwnerWithMetadata<T> Nest()
    {
        return new ReadOnlySpanOwnerWithMetadata<T>(_owner,
            QueryDepth == DatabaseQueryDepth ? QueryDepth : (ushort)(QueryDepth + 1));
    }
}

public static class ReadOnlySpanOwnerExtensions
{
    public static ReadOnlySpanOwnerWithMetadata<T> WithDepth<T>(this ReadOnlySpanOwner<T> owner, ushort depth) =>
        new(owner, depth);

    public static ReadOnlySpanOwnerWithMetadata<T> FromDatabase<T>(this ReadOnlySpanOwner<T> owner) =>
        new(owner, ReadOnlySpanOwnerWithMetadata<T>.DatabaseQueryDepth);
}
