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
}