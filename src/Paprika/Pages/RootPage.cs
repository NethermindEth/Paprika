using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

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
        public const int Size = Page.PageSize - PageHeader.Size;

        [FieldOffset(0)] public uint BlockNumber;

        [FieldOffset(sizeof(uint))] public Keccak BlockHash;

        [FieldOffset(sizeof(uint) + Keccak.Size)] public DbAddress NextFreePage;

        [FieldOffset(sizeof(uint) + Keccak.Size + DbAddress.Size)] public DbAddress DataPage;

        public DbAddress GetNextFreePage()
        {
            var free = NextFreePage;
            NextFreePage = NextFreePage.Next;
            return free;
        }
    }
}

