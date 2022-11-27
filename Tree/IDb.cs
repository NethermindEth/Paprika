namespace Tree;

public interface IDb
{
    ReadOnlySpan<byte> Read(long id);
    long Write(ReadOnlySpan<byte> payload);
}