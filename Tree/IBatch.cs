namespace Tree;

public interface IBatch
{
    void Set(ReadOnlySpan<byte> key, ReadOnlySpan<byte> value);

    bool TryGet(ReadOnlySpan<byte> key, out ReadOnlySpan<byte> value);

    void Commit();
}