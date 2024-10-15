namespace Paprika.Data;

public interface INibbleSelector
{
    static abstract bool Should(byte nibble);
}

public readonly struct AllNibblesSelector : INibbleSelector
{
    public static bool Should(byte nibble) => true;
}

public readonly struct LowerHalfSelector : INibbleSelector
{
    public static bool Should(byte nibble) => nibble < Constants.NibbleInHalf;
}

public readonly struct UpperHalfSelector : INibbleSelector
{
    public static bool Should(byte nibble) => nibble >= Constants.NibbleInHalf;
}

file static class Constants
{
    public const int NibbleInHalf = 8;
}