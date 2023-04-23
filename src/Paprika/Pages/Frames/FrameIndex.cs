namespace Paprika.Pages.Frames;

/// <summary>
/// Provides a wrapping for a <see cref="byte"/> based index of a <see cref="IFrame"/> on the page,
/// so that 0 can be used as a value and is different from the null value. 
/// </summary>
public readonly struct FrameIndex
{
    public static readonly FrameIndex Null = default;

    private const byte Shift = 1;
    private readonly byte _raw;

    private FrameIndex(byte raw)
    {
        _raw = raw;
    }

    public static FrameIndex FromIndex(byte index) => new((byte)(index + Shift));

    public static FrameIndex FromRaw(byte index) => new(index);

    /// <summary>
    /// Gets the value of the index.
    /// </summary>
    public byte Value => (byte)(_raw - Shift);

    /// <summary>
    /// Gets the raw value underneath.
    /// </summary>
    public byte Raw => _raw;

    public bool IsNull => _raw == 0;

    public override string ToString() => IsNull ? "null" : $"@{Value}";
}