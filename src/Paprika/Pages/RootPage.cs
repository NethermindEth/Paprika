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

    public RootPage(byte* ptr) : this(new Page(ptr))
    {
    }

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

        private const int AbandonedPagesStart = sizeof(uint) + Keccak.Size + DbAddress.Size + DbAddress.Size;

        /// <summary>
        /// This gives the upper boundary of the number of abandoned pages that can be kept in the list.
        /// </summary>
        /// <remarks>
        /// For the current values, it's 1012. With blocks happening every 12s, this should be enough to support
        /// a reader that last for over 3h.
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
        /// The actual root of the trie, the first page of the state.
        /// </summary>
        [FieldOffset(sizeof(uint) + Keccak.Size + DbAddress.Size)] public DbAddress DataPage;

        /// <summary>
        /// The start of the abandoned pages.
        /// </summary>
        [FieldOffset(sizeof(uint) + Keccak.Size + DbAddress.Size + DbAddress.Size)]
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
}

