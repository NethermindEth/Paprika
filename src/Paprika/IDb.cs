namespace Paprika;

public interface IDb
{
    /// <summary>
    /// Starts a db transaction that is for the next block.
    /// </summary>
    /// <returns>The transaction that handles block operations.</returns>
    ITransaction BeginNextBlock();
}