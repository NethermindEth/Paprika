using System.Diagnostics;

namespace Paprika.Pages;

/// <summary>
/// Represents an address in the database. It can be one of the following:
///
/// 1. an address page
/// 2. a data-frame page in the same page as the value resides in
/// </summary>
public readonly struct DbAddress : IEquatable<DbAddress>
{
    /// <summary>
    /// The null address.
    /// </summary>
    public static readonly DbAddress Null = default;

    public const int Size = sizeof(uint);

    /// <summary>
    /// This value is bigger <see cref="Pages.Page.PageCount"/> so that regular pages don't overflow.
    /// </summary>
    private const uint SamePage = 0x8000_0000;

    private const int ValueMask = 0x00FF_FFFF;

    /// <summary>
    /// The shift that is applied to the counter of the jumps captured by the address.
    /// </summary>
    private const int JumpCountShift = 24;

    private const uint JumpCounter = byte.MaxValue - (SamePage >> JumpCountShift);

    private readonly uint _value;

    /// <summary>
    /// Creates a database address that represents a jump to another frame within the same <see cref="Pages.Page"/>.
    /// </summary>
    /// <param name="frame">The frame to jump to.</param>
    /// <param name="previous">The previous jump within the same page.</param>
    /// <returns>A db address.</returns>
    public static DbAddress JumpToFrame(byte frame, DbAddress previous)
    {
        Debug.Assert(previous.IsSamePage || previous.IsNull, "Only same page chaining is allowed");

        var countShifted = (previous.SamePageJumpCount + 1) << JumpCountShift;

        return new(frame | SamePage | countShifted);
    }

    /// <summary>
    /// Creates a database address that represents a jump to another database <see cref="Pages.Page"/>.
    /// </summary>
    /// <param name="page">The page to go to.</param>
    /// <returns></returns>
    public static DbAddress Page(uint page)
    {
        Debug.Assert(page < Pages.Page.PageCount, "The page number breached the PageCount maximum");
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

    public uint SamePageJumpCount => (_value & ~SamePage) >> JumpCountShift;

    public bool IsSamePage => (_value & SamePage) == SamePage;

    // ReSharper disable once MergeIntoPattern
    public bool IsValidAddressPage => _value < Pages.Page.PageCount;

    public bool TryGetSamePage(out byte frame)
    {
        if (IsSamePage)
        {
            frame = (byte)(_value & ValueMask);
            return true;
        }

        frame = default;
        return false;
    }

    public static implicit operator uint(DbAddress address) => address._value;
    public static implicit operator int(DbAddress address) => (int)address._value;

    public override string ToString() => IsNull ? "null" :
        IsSamePage ? $"Jump within page to index: {ValueMask & _value}" : $"Page @{_value}";

    public bool Equals(DbAddress other) => _value == other._value;

    public override bool Equals(object? obj) => obj is DbAddress other && Equals(other);

    public override int GetHashCode() => (int)_value;
}