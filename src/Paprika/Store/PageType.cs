using Paprika.Crypto;

namespace Paprika.Store;

public enum PageType : byte
{
    None = 0,

    /// <summary>
    /// A standard Paprika page with a fan-out of 16.
    /// </summary>
    Standard = 1,

    /// <summary>
    /// <see cref="FanOutPage"/> that stores the mapping between <see cref="Keccak"/> and <see cref="DbAddress"/>
    /// where the storage belongs to.
    /// </summary>
    StorageMapping = 2,

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
    /// <see cref="StorageRootPage"/>
    /// </summary>
    StorageRoot = 6,
}

public interface IPageTypeProvider
{
    public static abstract PageType Type { get; }
}

public readonly struct StandardType : IPageTypeProvider
{
    public static PageType Type => PageType.Standard;
}

public readonly struct StorageMapping : IPageTypeProvider
{
    public static PageType Type => PageType.StorageMapping;
}
