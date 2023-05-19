namespace Paprika;

public interface IDb
{
    /// <summary>
    /// Starts a db transaction that is for the next block.
    /// </summary>
    /// <returns>The transaction that handles block operations.</returns>
    IBatch BeginNextBatch();

    /// <summary>
    /// Starts a readonly batch that preserves a snapshot of the database as in the moment of its creation.
    /// </summary>
    /// <returns></returns>
    IReadOnlyBatch BeginReadOnlyBatch();
}