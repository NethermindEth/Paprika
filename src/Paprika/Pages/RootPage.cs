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
        /// with <see cref="MemoryPage"/>.
        /// </summary>
        [FieldOffset(sizeof(uint) + Keccak.Size)] public DbAddress NextFreePage;

        /// <summary>
        /// The actual root of the trie, the first page of the state.
        /// </summary>
        [FieldOffset(sizeof(uint) + Keccak.Size + DbAddress.Size)] public DbAddress DataPage;

        /// <summary>
        /// The memory managing page. The one used to keep the list of abandon pages that might be used in the future
        /// for writes.
        /// </summary>
        [FieldOffset(sizeof(uint) + Keccak.Size + DbAddress.Size + DbAddress.Size)] public DbAddress MemoryPage;

        public DbAddress GetNextFreePage()
        {
            var free = NextFreePage;
            NextFreePage = NextFreePage.Next;
            return free;
        }
    }
}

