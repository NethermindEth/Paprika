using System.IO.MemoryMappedFiles;
using static System.Runtime.CompilerServices.Unsafe;

namespace Tree;

public class PersistentDb : IDb
{
    private const int ChunkSize = 256 * 1024 * 1024;
    private readonly List<File> _files = new();
    
    private Chunk _current;
    private int _currentNumber;
    
    public PersistentDb()
    {
        BuildFile();
    }

    public ReadOnlySpan<byte> Read(long id)
    {
        var (position, lenght, file) = Id.Decode(id);

        var chunk = (file == _currentNumber) ? _current : _files[file].Chunk;
        
        return chunk.Read(position, lenght);
    }

    public long Write(ReadOnlySpan<byte> payload)
    {
        if (_current.TryWrite(payload, out var position))
        {
            return Id.Encode(position, payload.Length, _currentNumber);
        }

        Flush();

        while (!_current.TryWrite(payload, out position))
        {
            Flush();
        }
        
        return Id.Encode(position, payload.Length, _currentNumber);
    }

    private void Flush()
    {
        var last = _files.Last();
        last.Flush();

        BuildFile(last.Number);
    }

    private void BuildFile(int number = -1)
    {
        var file = new File(number + 1);
        _files.Add(file);
        
        _current = file.Chunk;
        _currentNumber = file.Number;
    }

    private static string FormatName(int number) => $"{number:D6}.db";

    private unsafe class File : IDisposable
    {
        public readonly int Number;
        private readonly MemoryMappedFile _mapped;
        private readonly MemoryMappedViewAccessor _accessor;
        private readonly byte* _pointer;

        public File(int number)
        {
            Number = number;
            var name = FormatName(number);
            _mapped = MemoryMappedFile.CreateOrOpen(name, ChunkSize, MemoryMappedFileAccess.ReadWrite);
            _accessor = _mapped.CreateViewAccessor();
            _accessor.SafeMemoryMappedViewHandle.AcquirePointer(ref _pointer);
        }

        public Chunk Chunk => new(_pointer);
        
        public void Flush()
        {
            _accessor.Flush();
            
        }

        public void Dispose()
        {
            _accessor.SafeMemoryMappedViewHandle.ReleasePointer();
            _accessor.Dispose();
            _mapped.Dispose();
        }
    }

    private readonly struct Chunk
    {
        private readonly unsafe void* _ptr;
        private const int Preamble = 4;

        public unsafe Chunk(void* ptr)
        {
            _ptr = ptr;
        }

        public unsafe ReadOnlySpan<byte> Read(int position, int length) => new(Add<byte>(_ptr, position), length);

        public bool TryWrite(ReadOnlySpan<byte> data, out int position)
        {
            unsafe
            {
                // always add preamble so that the file does not overwrite it
                var current = ReadUnaligned<int>(_ptr) + Preamble;
                
                var destination = new Span<byte>(Add<byte>(_ptr, current), ChunkSize - current);

                var written = data.TryCopyTo(destination);

                if (written)
                {
                    WriteUnaligned(_ptr, current + data.Length);
                    position = current;
                    return true;
                }

                position = default;
                return false;
            }
        }
    }
}