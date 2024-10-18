namespace Paprika.Chain;

/// <summary>
/// Used as metadata to differentiate between entries stored in blocks. 
/// </summary>
public enum EntryType : byte
{
    /// <summary>
    /// A regular entry that should be persisted in the database.
    /// </summary>
    Persistent = 0,

    /// <summary>
    /// An entry representing data that were cached on the behalf of the decision of <see cref="CacheBudget"/>.
    /// </summary>
    Cached = 1,

    /// <summary>
    /// The entry is put only for a short period of computation and should not be considered to be stored in memory beyond this computation.
    /// </summary>
    UseOnce = 2,

    /// <summary>
    /// Entry used by snap sync to add keccak of a given path from proof. Used in calculating merkle, but not persisted
    /// </summary>
    Proof = 3,
}
