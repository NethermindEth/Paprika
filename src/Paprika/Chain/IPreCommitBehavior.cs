using Paprika.Data;
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
    public void BeforeCommit(ICommit commit);
}

/// <summary>
/// Provides the set of changes applied onto <see cref="IWorldState"/>,
/// allowing for additional modifications of the data just before the commit.
/// </summary>
/// <remarks>
/// Use Enumerator to access all the keys
///
/// public static void Foreach(this ICommit commit)
/// {
///     foreach (var key in commit)
///     {
///         key.
///     }
///  }
/// </remarks>
public interface ICommit
{
    /// <summary>
    /// Tries to retrieve the result stored under the given key only from this commit.
    /// </summary>
    /// <remarks>
    /// If successful, returns a result as an owner. Must be disposed properly.
    /// </remarks>
    public ReadOnlySpanOwner<byte> Get(in Key key);

    /// <summary>
    /// Sets the value under the given key.
    /// </summary>
    void Set(in Key key, in ReadOnlySpan<byte> payload);

    void Visit(CommitAction action);
}

/// <summary>
/// A delegate to be called on the each key that that the commit contains.
/// </summary>
public delegate void CommitAction(in Key key, ICommit commit);

