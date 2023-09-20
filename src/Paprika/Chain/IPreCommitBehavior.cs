using System.Buffers;
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
    object BeforeCommit(ICommit commit);

    /// <summary>
    /// Inspects the data allowing it to overwrite them if needed, before the commit is applied to the database.
    /// </summary>
    /// <param name="key">The key related to data.</param>
    /// <param name="data">The data.</param>
    /// <returns>The data that should be put in place.</returns>
    ReadOnlySpan<byte> InspectBeforeApply(in Key key, ReadOnlySpan<byte> data) => data;
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
    /// Tries to retrieve the result stored under the given key only from this commit.
    /// </summary>
    /// <remarks>
    /// If successful, returns a result as an owner. Must be disposed properly.
    /// </remarks>
    public ReadOnlySpanOwner<byte> Get(scoped in Key key);

    /// <summary>
    /// Sets the value under the given key.
    /// </summary>
    void Set(in Key key, in ReadOnlySpan<byte> payload);

    /// <summary>
    /// Sets the value under the given key.
    /// </summary>
    void Set(in Key key, in ReadOnlySpan<byte> payload0, in ReadOnlySpan<byte> payload1);

    /// <summary>
    /// Visits the given <paramref name="type"/> of the changes in the given commit.
    /// </summary>
    void Visit(CommitAction action, TrieType type) => throw new Exception("No visitor available for this commit");

    /// <summary>
    /// Gets the child commit that is a thread-safe write-through commit.
    /// </summary>
    /// <returns>A child commit.</returns>
    IChildCommit GetChild() => throw new Exception($"No {nameof(GetChild)} available for this commit");
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