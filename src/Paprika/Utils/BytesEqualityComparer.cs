namespace Paprika.Utils;

public class BytesEqualityComparer : IEqualityComparer<byte[]>
{
    public bool Equals(byte[]? x, byte[]? y)
    {
        if (ReferenceEquals(x, y))
            return true;

        if (x == null || y == null)
        {
            return false;
        }

        return x.SequenceEqual(y);
    }

    public int GetHashCode(byte[] obj)
    {
        HashCode hash = default;
        hash.AddBytes(obj);
        return hash.ToHashCode();
    }
}