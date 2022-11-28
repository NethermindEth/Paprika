namespace Tree;

public interface IDb
{
    ReadOnlySpan<byte> Read(long id);
    
    long Write(ReadOnlySpan<byte> payload);

    /// <summary>
    /// Marks the given can as free and no longer used.
    /// </summary>
    /// <param name="id"></param>
    void Free(long id);

    /// <summary>
    /// Write value in an updatable fashion, that is later accessible with
    /// </summary>
    /// <param name="payload"></param>
    /// <returns></returns>
    long WriteUpdatable(ReadOnlySpan<byte> payload);

    /// <summary>
    /// Tries to get the updatable under given address.
    /// </summary>
    /// <returns></returns>
    bool TryGetUpdatable(long id, out Span<byte> span);

    public void Seal();
}