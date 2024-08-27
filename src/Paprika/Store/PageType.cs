namespace Paprika.Store;

public enum PageType : byte
{
    None = 0,

    /// <summary>
    /// <see cref="Store.DataPage"/>.
    /// </summary>
    DataPage = 1,

    /// <summary>
    /// <see cref="StateRootPage"/>
    /// </summary>
    StateRoot = 2,

    /// <summary>
    /// Represents <see cref="AbandonedPage"/>
    /// </summary>
    Abandoned = 3,

    /// <summary>
    /// <see cref="LeafOverflowPage"/>
    /// </summary>
    LeafOverflow = 4,

    /// <summary>
    /// <see cref="StorageFanOut.Level1Page"/>
    /// </summary>
    FanOutPage = 5,
}