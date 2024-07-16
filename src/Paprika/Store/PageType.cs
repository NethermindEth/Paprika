namespace Paprika.Store;

public enum PageType : byte
{
    None = 0,

    /// <summary>
    /// A standard Paprika page with a fan-out of 16.
    /// </summary>
    Standard = 1,

    /// <summary>
    /// A page that is a Standard page but holds the account identity mappin.
    /// </summary>
    Identity = 2,

    /// <summary>
    /// Represents <see cref="AbandonedPage"/>
    /// </summary>
    Abandoned = 3,

    /// <summary>
    /// The leaf page that represents a part of the page.
    /// </summary>
    Leaf = 4,

    /// <summary>
    /// The overflow of the leaf, storing all the data.
    /// </summary>
    LeafOverflow = 5,
    
    /// <summary>
    /// Merkle FanOut Page
    /// </summary>
    MerkleFanOut = 6,
    
    /// <summary>
    /// Merkle Leaf
    /// </summary>
    MerkleLeaf = 7,
}

public interface IPageTypeProvider
{
    public static abstract PageType Type { get; }
}

public readonly struct StandardType : IPageTypeProvider
{
    public static PageType Type => PageType.Standard;
}

public readonly struct IdentityType : IPageTypeProvider
{
    public static PageType Type => PageType.Identity;
}
