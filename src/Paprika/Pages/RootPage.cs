using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Paprika.Crypto;

namespace Paprika.Pages;

/// <summary>
/// Root page is a page that contains all the needed metadata from the point of view of the database.
/// It also includes the blockchain information like block hash or block number
/// </summary>
public readonly unsafe struct RootPage : IPage
{
    private readonly Page _page;

    public RootPage(Page root) => _page = root;

    public ref PageHeader Header => ref _page.Header;

    public ref Payload Data => ref Unsafe.AsRef<Payload>(_page.Payload);


    /// <summary>
    /// Represents the data of the page.
    /// </summary>
    [StructLayout(LayoutKind.Explicit, Size = Size)]
    public struct Payload
    {
        private const int Size = Page.PageSize - PageHeader.Size;

        /// <summary>
        /// How big is the fan out for the root.
        /// </summary>
        private const int AccountPageFanOut = 256;

        /// <summary>
        /// The number of nibbles that are "consumed" on the root level.
        /// </summary>
        public const byte RootNibbleLevel = 2;

        private const int AbandonedPagesStart = sizeof(uint) + Keccak.Size + DbAddress.Size + DbAddress.Size * AccountPageFanOut;

        /// <summary>
        /// This gives the upper boundary of the number of abandoned pages that can be kept in the list.
        /// </summary>
        /// <remarks>
        /// The value is dependent on <see cref="AccountPageFanOut"/> as the more data pages addresses, the less space for
        /// the abandoned. Still, the number of abandoned that is required is ~max reorg depth as later, pages are reused.
        /// Even with fan-out of data pages equal to 256, there's still a lot of room here.
        /// </remarks>
        private const int AbandonedPagesCount = (Size - AbandonedPagesStart) / DbAddress.Size;

        /// <summary>
        /// The block number that the given batch represents.
        /// </summary>
        [FieldOffset(0)] public uint BlockNumber;

        /// <summary>
        /// The hash of the state root of the given block identified by <see cref="BlockNumber"/>.
        /// </summary>
        [FieldOffset(sizeof(uint))] public Keccak StateRootHash;

        /// <summary>
        /// The address of the next free page. This should be used rarely as pages should be reused
        /// with <see cref="AbandonedPage"/>.
        /// </summary>
        [FieldOffset(sizeof(uint) + Keccak.Size)] public DbAddress NextFreePage;

        /// <summary>
        /// The first of the data pages.
        /// </summary>
        [FieldOffset(sizeof(uint) + Keccak.Size + DbAddress.Size)] private DbAddress AccountPage;

        /// <summary>
        /// Gets the span of account pages of the root
        /// </summary>
        public Span<DbAddress> AccountPages => MemoryMarshal.CreateSpan(ref AccountPage, AccountPageFanOut);

        /// <summary>
        /// The start of the abandoned pages.
        /// </summary>
        [FieldOffset(AbandonedPagesStart)]
        private DbAddress AbandonedPage;

        /// <summary>
        /// Gets the span of the abandoned pages roots.
        /// </summary>
        public Span<DbAddress> AbandonedPages => MemoryMarshal.CreateSpan(ref AbandonedPage, AbandonedPagesCount);

        public DbAddress GetNextFreePage()
        {
            var free = NextFreePage;
            NextFreePage = NextFreePage.Next;
            return free;
        }
    }

    public static ref DbAddress FindAccountPage(Span<DbAddress> dataPages, in Keccak key)
    {
        var b = key.Span[0];
        return ref dataPages[b];
    }

    public void Accept(IPageVisitor visitor, IPageResolver resolver)
    {
        foreach (var dataAddr in Data.AccountPages)
        {
            if (dataAddr.IsNull == false)
            {
                var data = new FanOut256Page(resolver.GetAt(dataAddr));
                visitor.On(data, dataAddr);
            }
        }

        foreach (var addr in Data.AbandonedPages)
        {
            if (addr.IsNull == false)
            {
                var abandoned = new AbandonedPage(resolver.GetAt(addr));
                visitor.On(abandoned, addr);

                abandoned.Accept(visitor, resolver);
            }
        }
    }
}

