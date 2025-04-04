using System.Buffers;
using System.Diagnostics;
using System.Diagnostics.Metrics;
using System.IO.MemoryMappedFiles;
using System.Runtime.InteropServices;
using Microsoft.Win32.SafeHandles;
using Paprika.Utils;

namespace Paprika.Store.PageManagers;

public sealed class MemoryMappedPageManager : PointerPageManager
{
    private readonly PersistenceOptions _options;

    /// <summary>
    /// The only option is random access. As Paprika jumps over the file, any prefetching is futile.
    /// Also, the file cannot be async to use some of the mmap features. So here it is, random access file.
    /// </summary>
    private const FileOptions PaprikaFileOptions = FileOptions.RandomAccess | FileOptions.Asynchronous;

    private const string PaprikaFileName = "paprika.db";

    private readonly SafeFileHandle _file;
    private readonly MemoryMappedFile _mapped;
    private readonly MemoryMappedViewAccessor _whole;
    private readonly unsafe byte* _ptr;

    // Flusher section
    private readonly Stack<PageMemoryOwner> _owners = new();
    private readonly List<PageMemoryOwner> _ownersUsed = new();
    private readonly List<Task> _pendingWrites = new();
    private DbAddress[] _toWrite = new DbAddress[1];

    // Metrics
    private readonly Meter _meter;
    private readonly Histogram<int> _fileWrites;
    private readonly Histogram<int> _writeTime;

    public unsafe MemoryMappedPageManager(long size, byte historyDepth, string dir,
        PersistenceOptions options = PersistenceOptions.FlushFile) : base(size)
    {
        Path = GetPaprikaFilePath(dir);

        if (!File.Exists(Path))
        {
            var directory = System.IO.Path.GetDirectoryName(Path);
            if (!Directory.Exists(directory))
            {
                Directory.CreateDirectory(directory);
            }
            _file = File.OpenHandle(Path, FileMode.CreateNew, FileAccess.ReadWrite, FileShare.ReadWrite, PaprikaFileOptions);

            // set length
            RandomAccess.SetLength(_file, size);

            // clear first pages to make it clean
            var page = new byte[Page.PageSize];
            for (var i = 0; i < historyDepth; i++)
            {
                RandomAccess.Write(_file, page, i * Page.PageSize);
            }

            RandomAccess.FlushToDisk(_file);
        }
        else
        {
            _file = File.OpenHandle(Path, FileMode.Open, FileAccess.ReadWrite, FileShare.ReadWrite, PaprikaFileOptions);
        }

        _mapped = MemoryMappedFile.CreateFromFile(_file, null, size, MemoryMappedFileAccess.ReadWrite,
            HandleInheritability.None, true);

        _whole = _mapped.CreateViewAccessor();
        _whole.SafeMemoryMappedViewHandle.AcquirePointer(ref _ptr);
        _options = options;

        _meter = new Meter("Paprika.Store.PageManager");
        _fileWrites = _meter.CreateHistogram<int>("File writes", "Syscall", "Actual numbers of file writes issued");
        _writeTime = _meter.CreateHistogram<int>("Write time", "ms", "Time spent in writing");
    }

    public static string GetPaprikaFilePath(string dir) => System.IO.Path.Combine(dir, PaprikaFileName);

    public string Path { get; }

    protected override unsafe void* Ptr => _ptr;

    public override async ValueTask WritePages(ICollection<DbAddress> dbAddresses, CommitOptions options)
    {
        if (_options == PersistenceOptions.MMapOnly)
            return;

        if (options != CommitOptions.DangerNoWrite)
        {
            ScheduleWrites(dbAddresses);
            await AwaitWrites();
        }

        if (options != CommitOptions.DangerNoFlush && options != CommitOptions.DangerNoWrite)
        {
            RandomAccess.FlushToDisk(_file);
        }
    }

    /// <summary>
    /// The amount of pages that can be combined in a single write.
    /// </summary>
    private const int MaxWriteBatch = 64;

    private void ScheduleWrites(ICollection<DbAddress> dbAddresses)
    {
        var count = dbAddresses.Count;
        if (count == 0)
            return;

        if (_toWrite.Length < count)
        {
            Array.Resize(ref _toWrite, count);
        }

        dbAddresses.CopyTo(_toWrite, 0);
        var span = _toWrite.AsSpan(0, count);

        // raw sorting, to make writes ordered
        var numbers = MemoryMarshal.Cast<DbAddress, uint>(span);
        numbers.Sort();

        foreach (var range in numbers.BatchConsecutive(MaxWriteBatch))
        {
            _pendingWrites.Add(WriteAt(new DbAddress(range.Start), (uint)range.Length).AsTask());
        }

        _fileWrites.Record(_pendingWrites.Count);
    }

    private ValueTask WriteAt(DbAddress addr, uint count = 1)
    {
        var page = GetAt(addr);
        return RandomAccess.WriteAsync(_file, Own(page, count).Memory, addr.FileOffset);
    }

    private async Task AwaitWrites()
    {
        var writes = Stopwatch.StartNew();

        await Task.WhenAll(_pendingWrites);
        ReleaseOwners();
        _pendingWrites.Clear();

        _writeTime.Record((int)writes.ElapsedMilliseconds);
    }

    public override Page GetAtForWriting(DbAddress address, bool reused) => GetAt(address);

    public override async ValueTask WriteRootPage(DbAddress root, CommitOptions options)
    {
        if (_options == PersistenceOptions.MMapOnly)
            return;

        if (options != CommitOptions.DangerNoWrite)
        {
            await WriteAt(root);
        }

        if (options == CommitOptions.FlushDataAndRoot)
        {
            RandomAccess.FlushToDisk(_file);
        }
    }

    public override void Flush()
    {
        if (_options == PersistenceOptions.MMapOnly)
            return;

        RandomAccess.FlushToDisk(_file);
    }

    public override void ForceFlush()
    {
        _whole.Flush();
        RandomAccess.FlushToDisk(_file);
    }

    public override bool UsesPersistentPaging => _options == PersistenceOptions.FlushFile;

    public override void Dispose()
    {
        _meter.Dispose();

        _whole.SafeMemoryMappedViewHandle.ReleasePointer();
        _whole.Dispose();
        _mapped.Dispose();
        _file.Dispose();
    }

    private PageMemoryOwner Own(Page page, uint count)
    {
        if (_owners.TryPop(out var owner) == false)
        {
            owner = new();
        }

        _ownersUsed.Add(owner);

        owner.Page = page;
        owner.Count = count;

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
        public uint Count;

        protected override void Dispose(bool disposing)
        {
        }

        public override unsafe Span<byte> GetSpan() => new(Page.Raw.ToPointer(), (int)(Page.PageSize * Count));

        public override unsafe MemoryHandle Pin(int elementIndex = 0) =>
            new((byte*)Page.Raw.ToPointer() + elementIndex);

        public override void Unpin()
        {
        }
    }
}

public enum PersistenceOptions
{
    /// <summary>
    /// Do FSYNC/FlushFileBuffers
    /// </summary>
    FlushFile,

    /// <summary>
    /// Don't issue any writes or sync.
    /// </summary>
    MMapOnly,
}
