using System.Numerics;

namespace Paprika.Pages;

/// <summary>
/// Represents any index that wants to use 0, the default value as null and shift all the other values by one.
/// </summary>
/// <typeparam name="TPrimitive"></typeparam>
public readonly struct Index<TPrimitive>
    where TPrimitive : INumberBase<TPrimitive>
{
    // ReSharper disable once StaticMemberInGenericType
    public static readonly Index<TPrimitive> Null = default;

    private static readonly TPrimitive Shift = TPrimitive.One;
    private readonly TPrimitive _raw;

    private Index(TPrimitive raw)
    {
        _raw = raw;
    }

    public static Index<TPrimitive> FromIndex(TPrimitive index) => new(index + Shift);

    public static Index<TPrimitive> FromRaw(TPrimitive index) => new(index);

    /// <summary>
    /// Gets the value of the index.
    /// </summary>
    public TPrimitive Value => _raw - Shift;

    /// <summary>
    /// Gets the raw value underneath.
    /// </summary>
    public TPrimitive Raw => _raw;

    public bool IsNull => _raw == TPrimitive.Zero;

    public override string ToString() => IsNull ? "null" : $"@{Value}";
}