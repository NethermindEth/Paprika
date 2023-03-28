using System.Runtime.CompilerServices;

namespace Paprika.Pages;

/// <summary>
/// Root page is a page that contains all the needed metadata from the point of view of the database.
/// It also includes the blockchain information like <see cref="BlockNumber"/> and
/// <see cref="BlockHash"/>.
/// </summary>
public readonly unsafe struct RootPage : IPage
{
    private readonly Page _page;

    public RootPage(byte* ptr) : this(new Page(ptr))
    {
    }

    public RootPage(Page root) => _page = root;

    public ref PageHeader Header => ref _page.Header;

    public ref uint BlockNumber => ref Unsafe.AsRef<uint>(_page.Payload);

    public ref Keccak BlockHash => ref Unsafe.AsRef<Keccak>(_page.Payload + sizeof(uint));

    public ref int NextFreePage => ref Unsafe.AsRef<int>(_page.Payload + sizeof(uint) + sizeof(Keccak));
}

