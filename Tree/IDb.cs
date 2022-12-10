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
    /// Tries to get the updatable under given address.
    /// </summary>
    /// <returns></returns>
    bool TryGetUpdatable(long id, out Span<byte> span);

    /// <summary>
    /// Starts the upgradable region.
    /// </summary>
    void StartUpgradableRegion();

    public void Seal();
}