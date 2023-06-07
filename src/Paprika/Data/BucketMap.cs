namespace Paprika.Data;

/// <summary>
/// Provides a simple capability of storing up values in buckets, differentiated by the first nibble of the path.
/// Effectively, this map has only <see cref="BucketCount"/> slots, but this should be more than enough to
/// amortize writes in the tree.
/// </summary>
public readonly ref struct BucketMap
{
    public const int TotalSize = BucketCount * BucketSize;
    public const int BucketCount = 16;
    private const int BucketSize = 32 + 32 + 96; // key + storage key + 96 for data and others

    private readonly Span<byte> _data;
    
    public BucketMap(Span<byte> buffer)
    {
        _data = buffer;
    }

    public void Clear() => _data.Clear();

    /// <summary>
    /// Tries to retrieve the existing data by the nibble.
    /// </summary>
    public bool TryGetByNibble(byte nibble, out Key key, out ReadOnlySpan<byte> data)
    {
        var slice = GetNibbleBucket(nibble);

        if (slice[0] == 0)
        {
            // no nibble path stored
            key = default;
            data = default;
            
            return false;
        }

        var leftover = NibblePath.ReadFrom(slice, out var path);
        var type = (DataType)leftover[0];
        leftover = ReadSpan(leftover.Slice(1), out var additionalKey);

        key = new Key(path, type, additionalKey);
        ReadSpan(leftover, out data);
        
        return true;
    }

    /// <summary>
    /// Overwrites whatever is in the map under the given key.
    /// </summary>
    public void Set(in Key key, ReadOnlySpan<byte> data)
    {
        var slice = GetNibbleBucket(key.Path.FirstNibble);
        
        slice.Clear();

        var leftover = key.Path.WriteToWithLeftover(slice);
        leftover[0] = (byte)key.Type;
        leftover = WriteSpan(key.AdditionalKey, leftover.Slice(1));
        WriteSpan(data, leftover);
    }

    private Span<byte> GetNibbleBucket(byte nibble) => _data.Slice(nibble * BucketSize, BucketSize);

    private const int LengthOfLength = 1;
    
    private static ReadOnlySpan<byte> ReadSpan(ReadOnlySpan<byte> span, out ReadOnlySpan<byte> result)
    {
        var length = span[0];
        result = span.Slice(LengthOfLength, length);
        return span.Slice(LengthOfLength + length);
    }
    
    private static Span<byte> WriteSpan(ReadOnlySpan<byte> span, Span<byte> destination)
    {
        destination[0] = (byte)span.Length;
        span.CopyTo(destination.Slice(LengthOfLength));
        return destination.Slice(LengthOfLength + span.Length);
    }
}