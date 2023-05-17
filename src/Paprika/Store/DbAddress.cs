using System.Buffers.Binary;
using System.Diagnostics;

namespace Paprika.Store;

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

    // private const uint JumpCounter = byte.MaxValue - (SamePage >> JumpCountShift);
    private const int NullValue = 0;

    private readonly uint _value;

    public uint Raw => _value;

    /// <summary>
    /// Creates a database address that represents a jump to another database <see cref="Store.Page"/>.
    /// </summary>
    /// <param name="page">The page to go to.</param>
    /// <returns></returns>
    public static DbAddress Page(uint page)
    {
        Debug.Assert(page < Store.Page.PageCount, "The page number breached the PageCount maximum");
        return new(page);
    }

    /// <summary>
    /// Gets the next address.
    /// </summary>
    public DbAddress Next => new(_value + 1);

    public DbAddress(uint value) => _value = value;

    public bool IsNull => _value == NullValue;

    // ReSharper disable once MergeIntoPattern
    public bool IsValidPageAddress => _value < Store.Page.PageCount;

    public static implicit operator uint(DbAddress address) => address._value;
    public static implicit operator int(DbAddress address) => (int)address._value;

    public override string ToString() => IsNull ? "null" : $"Page @{_value}";

    public bool Equals(DbAddress other) => _value == other._value;

    public override bool Equals(object? obj) => obj is DbAddress other && Equals(other);

    public override int GetHashCode() => (int)_value;

    public static DbAddress Read(ReadOnlySpan<byte> data) => new(BinaryPrimitives.ReadUInt32LittleEndian(data));

    public void Write(Span<byte> destination) => BinaryPrimitives.WriteUInt32LittleEndian(destination, _value);
}