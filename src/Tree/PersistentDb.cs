using System.IO.MemoryMappedFiles;
using static System.Runtime.CompilerServices.Unsafe;

namespace Tree;

public class PersistentDb : IDb
{
    private readonly string _dir;
    public const string Ext = "db";
    
    private const int ChunkSize = 1024 * 1024 * 1024;
    private readonly List<File> _files = new();
    private readonly int[] _used = new int[1000]; // dummy constant

    private Chunk _current;
    private int _currentNumber;

    public PersistentDb(string dir)
    {
        _dir = dir;
        BuildFile();
    }

    public Span<byte> Read(long id)
    {
        var decoded = Id.Decode(id);

        var chunk = decoded.File == _currentNumber ? _current : _files[decoded.File].Chunk;
        
        return chunk.Read(decoded.Position, decoded.Length);
    }

    public long Write(ReadOnlySpan<byte> payload)
    {
        if (_current.TryWrite(payload, out var position))
        {
            return Id.Encode(position, payload.Length, _currentNumber);
        }

        BuildNewFile();

        while (!_current.TryWrite(payload, out position))
        {
            BuildNewFile();
        }
        
        return Id.Encode(position, payload.Length, _currentNumber);
    }

    public void Free(long id)
    {
        var decoded = Id.Decode(id);
        _used[decoded.File] -= decoded.Length;
    }

    public long NextId => Id.Encode(_current.Position, 0, _currentNumber);
    
    public void FlushFrom(long id)
    {
        var decoded  = Id.Decode(id);
        for (int i = decoded.File; i < _files.Count; i++)
        {
            _files[i].Flush();
        }
    }

    public void PrintStats()
    {
        for (var i = 0; i <= _currentNumber; i++)
        {
            var percentage = (int)(((double)_used[i]) / ChunkSize * 100);
            Console.WriteLine($"File {i:D5} is used by the current root at {percentage}%");
        }
    }

    private void BuildNewFile()
    {
        var last = _files.Last();
        BuildFile(last.Number);
    }

    private void BuildFile(int number = -1)
    {
        var file = new File(_dir, number + 1);
        _files.Add(file);
        
        _current = file.Chunk;
        _currentNumber = file.Number;

        _used[_currentNumber] = ChunkSize - Chunk.Preamble;
    }

    private static string FormatName(string dir, int number) => Path.Combine(dir, $"{number:D6}.{Ext}");

    private unsafe class File : IDisposable
    {
        public readonly int Number;
        private readonly MemoryMappedFile _mapped;
        private readonly MemoryMappedViewAccessor _accessor;
        private readonly byte* _pointer;
        private readonly FileStream _file;

        public File(string dir, int number)
        {
            Number = number;
            var name = FormatName(dir, number);

            _file = new FileStream(name, FileMode.CreateNew, FileAccess.ReadWrite);
            _file.SetLength(ChunkSize);
            _mapped = MemoryMappedFile.CreateFromFile(_file, null, ChunkSize, MemoryMappedFileAccess.ReadWrite, HandleInheritability.None, false);
            _accessor = _mapped.CreateViewAccessor();
            _accessor.SafeMemoryMappedViewHandle.AcquirePointer(ref _pointer);
            
            Chunk.Initialize();
        }

        public Chunk Chunk => new(_pointer);
        
        public void Flush()
        {
            _accessor.Flush();
            _file.Flush(true);
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
        public const int Preamble = 4;

        public unsafe Chunk(void* ptr)
        {
            _ptr = ptr;
        }

        public unsafe void Initialize() => WriteUnaligned(_ptr, Preamble);

        public unsafe Span<byte> Read(int position, int length) => new(Add<byte>(_ptr, position), length);
        
        public unsafe int Position => ReadUnaligned<int>(_ptr);
        public bool TryWrite(ReadOnlySpan<byte> data, out int position)
        {
            unsafe
            {
                // always add preamble so that the file does not overwrite it
                var current = ReadUnaligned<int>(_ptr);
                
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