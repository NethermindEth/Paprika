using System.IO.MemoryMappedFiles;

namespace Paprika.Db;

public unsafe class MemoryMappedPagedDb : PagedDb
{
    private readonly string _path;

    private readonly FileStream _file;
    private readonly MemoryMappedFile _mapped;
    private readonly MemoryMappedViewAccessor _accessor;
    private readonly byte* _ptr;

    public MemoryMappedPagedDb(ulong size, string path, bool deleteOnStart = false) : base(size)
    {
        _path = path;

        var name = Path.Combine(_path, "memory-mapped.db");

        if (deleteOnStart && File.Exists(name))
            File.Delete(name);

        _file = new FileStream(name, FileMode.CreateNew, FileAccess.ReadWrite);
        _file.SetLength((long)size);
        _mapped = MemoryMappedFile.CreateFromFile(_file, null, (long)size, MemoryMappedFileAccess.ReadWrite,
            HandleInheritability.None, false);
        _accessor = _mapped.CreateViewAccessor();
        _accessor.SafeMemoryMappedViewHandle.AcquirePointer(ref _ptr);

        RootInit();
    }

    protected override void* Ptr => _ptr;

    public override void Dispose()
    {
        _accessor.Flush();
        _accessor.SafeMemoryMappedViewHandle.ReleasePointer();
        _accessor.Dispose();
        _mapped.Dispose();
    }

    protected override void Flush()
    {
        _accessor.Flush();
        _file.Flush(true);
    }
}