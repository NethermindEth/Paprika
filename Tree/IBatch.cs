namespace Tree;

public interface IBatch
{
    void Set(in ReadOnlySpan<byte> key, in ReadOnlySpan<byte> value);

    bool TryGet(in ReadOnlySpan<byte> key, out ReadOnlySpan<byte> value);

    /// <summary>
    /// Commits the given batch.
    /// </summary>
    void Commit(CommitOptions options = CommitOptions.ForceFlush);
}

public enum CommitOptions
{
    /// <summary>
    /// The commit updates root only, leaving all the nodes updatable and not committed to disk.
    /// </summary>
    RootOnly,

    /// <summary>
    /// The commit updates root as <see cref="RootOnly"/> but also calculates its hash.
    /// </summary>
    RootOnlyWithHash,

    /// <summary>
    /// Seals the updatable making them readonly but does no flush to disk. 
    /// </summary>
    SealUpdatable,

    /// <summary>
    /// Forces the flush to disk. It also runs <see cref="SealUpdatable"/>.
    /// </summary>
    ForceFlush,
}