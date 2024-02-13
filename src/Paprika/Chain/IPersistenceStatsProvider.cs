namespace Paprika.Chain;

/// <summary>
/// Provides statistics about persistence aspects.
/// </summary>
public interface IPersistenceStatsProvider
{
    /// <summary>
    /// Db reads performed by this component since it's start of the lifetime.
    /// </summary>
    public int DbReads { get; }
}