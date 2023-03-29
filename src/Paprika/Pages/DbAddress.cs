using System.Diagnostics;

namespace Paprika.Pages;

/// <summary>
/// Represents a page address in the db.
/// </summary>
public readonly struct DbAddress
{
    public const int Size = sizeof(uint);

    /// <summary>
    /// This value is bigger <see cref="Page.PageCount"/> so that regular pages don't overflow.
    /// </summary>
    private const uint SamePage = 0x1000_0000;

    private readonly uint _value;

    /// <summary>
    /// Creates a database address that represents a jump to another frame within the same <see cref="Page"/>.
    /// </summary>
    /// <param name="frame">The frame to jump to.</param>
    /// <returns>A db address.</returns>
    public static DbAddress JumpToFrame(byte frame) => new(frame | SamePage);

    /// <summary>
    /// Creates a database address that represents a jump to another database <see cref="Page"/>.
    /// </summary>
    /// <param name="page">The page to go to.</param>
    /// <returns></returns>
    public static DbAddress AnotherPage(uint page)
    {
        Debug.Assert(page < Page.PageCount, "The page number breached the PageCount maximum");
        return new(page);
    }

    /// <summary>
    /// Gets the next address.
    /// </summary>
    public DbAddress Next
    {
        get
        {
            Debug.Assert(IsSamePage == false,
                "The next operator should be used only of the page addresses, not same page addresses");
            return new DbAddress(_value + 1);
        }
    }

    private DbAddress(uint value) => _value = value;

    public bool IsNull => _value == 0;

    public bool IsSamePage => (_value & SamePage) == SamePage;
}