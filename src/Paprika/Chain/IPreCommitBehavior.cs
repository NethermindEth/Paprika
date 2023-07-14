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
    /// <returns>
    /// Whether the retrieval was successful.
    /// </returns>
    /// <remarks>
    /// If successful, returns a result as an owner. Must be disposed properly.
    /// </remarks>
    public bool TryGet(in Key key, out ReadOnlySpanOwner<byte> result);

    /// <summary>
    /// Sets the value under the given key.
    /// </summary>
    void Set(in Key key, in ReadOnlySpan<byte> payload);

    /// <summary>
    /// Gets the enumerator for the keys in the given commit.
    /// </summary>
    /// <returns></returns>
    IKeyEnumerator GetEnumerator();
}

/// <summary>
/// The <see cref="Key"/> enumerator.
/// </summary>
public interface IKeyEnumerator : IDisposable
{
    /// <summary>
    /// The current key.
    /// </summary>
    ref readonly Key Current { get; }

    /// <summary>
    /// Moves to the next.
    /// </summary>
    public bool MoveNext();
}

