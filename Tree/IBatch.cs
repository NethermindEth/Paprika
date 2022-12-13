namespace Tree;

public interface IBatch
{
    void Set(in ReadOnlySpan<byte> key, in ReadOnlySpan<byte> value);

    bool TryGet(in ReadOnlySpan<byte> key, out ReadOnlySpan<byte> value);

    void Commit();
}