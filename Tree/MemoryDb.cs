﻿using System.Runtime.InteropServices;

namespace Tree;

public unsafe class MemoryDb : IDb, IDisposable
{
    private const int FileNumber = 13;

    private long _upgradableFrom = long.MaxValue;
    private byte* _memory;
    public int Size { get; }
    public int Position { get; private set; }

    public MemoryDb(int size)
    {
        Size = size;
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

    public bool TryGetUpdatable(long id, out Span<byte> span)
    {
        if (id >= _upgradableFrom)
        {
            var (position, lenght, file) = Id.Decode(id);

            if (file != FileNumber)
                throw new ArgumentException($"Wrong file: {file}");

            span = new Span<byte>(_memory + position, lenght);
            return true;
        }

        span = default;
        return false;
    }

    public void StartUpgradableRegion()
    {
        _upgradableFrom = Id.Encode(Position, 0, FileNumber);
    }

    public void Seal()
    {
        _upgradableFrom = long.MaxValue;
    }

    public void Dispose()
    {
        NativeMemory.Free(_memory);
        _memory = default;
    }
}