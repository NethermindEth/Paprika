namespace Tree;

public interface IDb
{
    Span<byte> Read(long id);

    long Write(ReadOnlySpan<byte> payload);

    /// <summary>
    /// Marks the given can as free and no longer used.
    /// </summary>
    /// <param name="id"></param>
    void Free(long id);

    /// <summary>
    /// Returns the id that all the ids written to the database will be bigger than.
    /// </summary>
    long NextId { get; }

    public void FlushFrom(long id);
}