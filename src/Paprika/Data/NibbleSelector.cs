namespace Paprika.Data;

/// <summary>
/// Gets a nibble from the key.
/// </summary>
public delegate byte NibbleSelector(ReadOnlySpan<byte> key);