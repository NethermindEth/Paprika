using System.Runtime.InteropServices;

namespace Tree.Tests.Mocks;

public unsafe class TestMemoryDb : IDb, IDisposable
{
    private const int FileNumber = 13;
    
    private byte* _memory;
    private int _position;

    public TestMemoryDb(int size)
    {
        _memory = (byte*)NativeMemory.Alloc((UIntPtr)size);
    }

    public ReadOnlySpan<byte> Read(long id)
    {
        var (position, lenght, file) = Id.Decode(id);

        if (file != FileNumber)
            throw new ArgumentException($"Wrong file: {file}");

        return new ReadOnlySpan<byte>(_memory + position, lenght);
    }

    public long Write(ReadOnlySpan<byte> payload)
    {
        var length = payload.Length;
        payload.CopyTo(new Span<byte>(_memory + _position, length));
        var key= Id.Encode(_position, length, FileNumber);
        
        _position += length;

        return key;
    }

    public void Dispose()
    {
        NativeMemory.Free(_memory);
        _memory = default;
    }
}