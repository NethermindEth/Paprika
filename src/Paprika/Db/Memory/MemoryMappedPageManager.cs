using System.IO.MemoryMappedFiles;
using Paprika.Data;

namespace Paprika.Db.Memory;

public unsafe class MemoryMappedPageManager : PointerPageManager
{
    /// <summary>
    /// The only option is random access. As Paprika jumps over the file, any prefetching is futile.
    /// Also, the file cannot be async to use some of the mmap features. So here it is, random access file. 
    /// </summary>
    private const FileOptions PaprikaFileOptions = FileOptions.RandomAccess;

    private readonly FileStream _file;
    private readonly MemoryMappedFile _mapped;
    private readonly MemoryMappedViewAccessor _whole;
    private readonly MemoryMappedViewAccessor _rootsOnly;
    private readonly byte* _ptr;

    public MemoryMappedPageManager(ulong size, byte historyDepth, string dir) : base(size)
    {
        Path = System.IO.Path.Combine(dir, "paprika.db");

        if (!File.Exists(Path))
        {
            _file = new FileStream(Path, FileMode.CreateNew, FileAccess.ReadWrite, FileShare.ReadWrite, 4096,
                PaprikaFileOptions);

            // set length
            _file.SetLength((long)size);

            // clear first pages to make it clean
            var page = new byte[Page.PageSize];
            for (var i = 0; i < historyDepth; i++)
            {
                _file.Write(page);
            }

            _file.Flush(true);
        }
        else
        {
            _file = new FileStream(Path, FileMode.Open, FileAccess.ReadWrite, FileShare.ReadWrite, 4096,
                PaprikaFileOptions);
        }

        _mapped = MemoryMappedFile.CreateFromFile(_file, null, (long)size, MemoryMappedFileAccess.ReadWrite,
            HandleInheritability.None, false);

        _whole = _mapped.CreateViewAccessor();
        _whole.SafeMemoryMappedViewHandle.AcquirePointer(ref _ptr);

        _rootsOnly = _mapped.CreateViewAccessor(0, historyDepth * Page.PageSize);
    }

    public string Path { get; }

    protected override void* Ptr => _ptr;

    public override void FlushAllPages()
    {
        // On Windows LMDB does not use FlushViewOfFile + FlushFileBuffers due to some performance gains.
        // At the moment, simplicity of using it wins over a much more complicated than scheduling writes with WriteFile.
        // see: https://github.com/LMDB/lmdb/blob/3947014aed7ffe39a79991fa7fb5b234da47ad1a/libraries/liblmdb/mdb.c#L3715-L3716
        // see: https://github.com/LMDB/lmdb/blob/3947014aed7ffe39a79991fa7fb5b234da47ad1a/libraries/liblmdb/mdb.c#L3775-L3784

        _whole.Flush();
        _file.Flush(true);
    }

    public override void FlushRootPage(in Page rootPage)
    {
        // for now, flush all root pages.
        // Adding LMDB-like flush that works with one page would require a lot of interop
        // https://github.com/LMDB/lmdb/blob/3947014aed7ffe39a79991fa7fb5b234da47ad1a/libraries/liblmdb/mdb.c#L4136

        _rootsOnly.Flush();
        _file.Flush(true);
    }

    public override void Dispose()
    {
        _whole.Flush();
        _whole.SafeMemoryMappedViewHandle.ReleasePointer();
        _whole.Dispose();
        _mapped.Dispose();
    }
}