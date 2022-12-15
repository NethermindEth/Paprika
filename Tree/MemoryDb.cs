using System.Runtime.InteropServices;

namespace Tree;

public unsafe class MemoryDb : IDb, IDisposable
{
    private const int FileNumber = 13;

    private byte* _memory;
    public int Size { get; }
    public int Position { get; private set; }

    public MemoryDb(int size)
    {
        Size = size;
        _memory = (byte*)NativeMemory.Alloc((UIntPtr)size);
    }

    public Span<byte> Read(long id)
    {
        var decoded = Id.Decode(id);

        if (decoded.File != FileNumber)
            throw new ArgumentException($"Wrong file: {decoded.File}");

        return new Span<byte>(_memory + decoded.Position, decoded.Length);
    }

    public long Write(ReadOnlySpan<byte> payload)
    {
        var length = payload.Length;

        if (Position + length > Size)
        {
            throw new Exception("Not enough memory!");
        }

        payload.CopyTo(new Span<byte>(_memory + Position, length));

        var key = Id.Encode(Position, length, FileNumber);

        Position += length;

        return key;
    }

    public void Free(long id)
    {
        // TODO: consider adding something here? Maybe a jump to the next value so that it can be flushed later?
    }

    public long NextId =>Id.Encode(Position, 0, FileNumber);
    
    public void FlushFrom(long id)
    {
    }

    public void Dispose()
    {
        NativeMemory.Free(_memory);
        _memory = default;
    }
}