using System.Buffers;
using System.IO.MemoryMappedFiles;
using Paprika.Data;

namespace Paprika.Store.PageManagers;

public class MemoryMappedPageManager : PointerPageManager
{
    /// <summary>
    /// The only option is random access. As Paprika jumps over the file, any prefetching is futile.
    /// Also, the file cannot be async to use some of the mmap features. So here it is, random access file. 
    /// </summary>
    private const FileOptions PaprikaFileOptions = FileOptions.RandomAccess | FileOptions.Asynchronous;

    private readonly FileStream _file;
    private readonly MemoryMappedFile _mapped;
    private readonly MemoryMappedViewAccessor _whole;
    private readonly unsafe byte* _ptr;

    // Flusher section
    private readonly Stack<PageMemoryOwner> _owners = new();
    private readonly List<PageMemoryOwner> _ownersUsed = new();
    private readonly List<Task> _pendingWrites = new();

    public unsafe MemoryMappedPageManager(ulong size, byte historyDepth, string dir) : base(size)
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
            _file = new FileStream(Path, FileMode.Open, FileAccess.ReadWrite, FileShare.None, 4096,
                PaprikaFileOptions);
        }

        _mapped = MemoryMappedFile.CreateFromFile(_file, null, (long)size, MemoryMappedFileAccess.ReadWrite,
            HandleInheritability.None, true);

        _whole = _mapped.CreateViewAccessor();
        _whole.SafeMemoryMappedViewHandle.AcquirePointer(ref _ptr);
    }

    public string Path { get; }

    protected override unsafe void* Ptr => _ptr;

    public override async ValueTask FlushPages(IReadOnlyCollection<DbAddress> dbAddresses, CommitOptions options)
    {
        if (options != CommitOptions.DangerNoWrite)
        {
            // TODO: remove alloc
            var addresses = dbAddresses.ToArray();
            Array.Sort(addresses, (a, b) => a.Raw.CompareTo(b.Raw));

            foreach (var addr in addresses)
            {
                // a regular address to write
                _pendingWrites.Add(WriteAt(addr).AsTask());
                await AwaitWrites();
            }
        }

        if (options != CommitOptions.DangerNoFlush && options != CommitOptions.DangerNoWrite)
        {
            _file.Flush(true);
        }
    }

    private ValueTask WriteAt(DbAddress addr)
    {
        var offset = addr.Raw * Page.PageSize;
        var page = GetAt(addr);
        return RandomAccess.WriteAsync(_file.SafeFileHandle, Own(page).Memory, offset);
    }

    private async Task AwaitWrites()
    {
        await Task.WhenAll(_pendingWrites);
        ReleaseOwners();
        _pendingWrites.Clear();
    }

    public override Page GetAtForWriting(DbAddress address, bool reused) => GetAt(address);

    public override async ValueTask FlushRootPage(DbAddress root, CommitOptions options)
    {
        if (options != CommitOptions.DangerNoWrite)
        {
            await WriteAt(root);
        }

        if (options == CommitOptions.FlushDataAndRoot)
        {
            _file.Flush(true);
        }
    }

    public override void Dispose()
    {
        _whole.SafeMemoryMappedViewHandle.ReleasePointer();
        _whole.Dispose();
        _mapped.Dispose();
        _file.Dispose();
    }

    private PageMemoryOwner Own(Page page)
    {
        if (_owners.TryPop(out var owner) == false)
        {
            owner = new();
        }

        _ownersUsed.Add(owner);

        owner.Page = page;
        return owner;
    }

    private void ReleaseOwners()
    {
        foreach (var used in _ownersUsed)
        {
            _owners.Push(used);
        }

        _ownersUsed.Clear();
    }

    private class PageMemoryOwner : MemoryManager<byte>
    {
        public Page Page;

        protected override void Dispose(bool disposing)
        {
        }

        public override unsafe Span<byte> GetSpan() => new(Page.Raw.ToPointer(), Page.PageSize);

        public override unsafe MemoryHandle Pin(int elementIndex = 0) =>
            new((byte*)Page.Raw.ToPointer() + elementIndex);

        public override void Unpin()
        {
        }
    }
}