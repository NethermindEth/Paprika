using System.Buffers;
using System.Diagnostics;
using System.Diagnostics.Metrics;
using System.IO.MemoryMappedFiles;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Threading.Channels;
using Paprika.Utils;

namespace Paprika.Store.PageManagers;

public sealed class MemoryMappedPageManager : PointerPageManager
{
    private readonly PersistenceOptions _options;

    /// <summary>
    /// As Paprika jumps to various addresses in the file, using <see cref="FileOptions.SequentialScan"/>
    /// would be harmful and <see cref="FileOptions.RandomAccess"/> is used.
    ///
    /// The file uses <see cref="FileOptions.Asynchronous"/> to issue proper async <see cref="WriteAt"/> operations. 
    /// </summary>
    private const FileOptions PaprikaFileOptions = FileOptions.RandomAccess | FileOptions.Asynchronous;

    private const string PaprikaFileName = "paprika.db";

    private readonly FileStream _file;
    private readonly MemoryMappedFile _mapped;
    private readonly MemoryMappedViewAccessor _whole;
    private readonly unsafe byte* _ptr;

    // Prefetcher
    private const int PrefetcherCapacity = 1000;
    private readonly Channel<DbAddress> _prefetches = Channel.CreateBounded<DbAddress>(new BoundedChannelOptions(PrefetcherCapacity)
    {
        FullMode = BoundedChannelFullMode.DropOldest,
        SingleReader = true,
        SingleWriter = false,
        Capacity = PrefetcherCapacity,
        AllowSynchronousContinuations = false
    });
    private readonly Task _prefetcher;

    // Flusher section
    private readonly Stack<PageMemoryOwner> _owners = new();
    private readonly List<PageMemoryOwner> _ownersUsed = new();
    private readonly List<Task> _pendingWrites = new();
    private DbAddress[] _toWrite = new DbAddress[1];

    private readonly Meter _meter;
    private readonly Histogram<int> _fileWrites;
    private readonly Histogram<int> _writeTime;

    public unsafe MemoryMappedPageManager(long size, byte historyDepth, string dir,
        PersistenceOptions options = PersistenceOptions.FlushFile) : base(size)
    {
        Path = GetPaprikaFilePath(dir);

        if (!File.Exists(Path))
        {
            _file = new FileStream(Path, FileMode.CreateNew, FileAccess.ReadWrite, FileShare.ReadWrite, 4096,
                PaprikaFileOptions);

            // set length
            _file.SetLength(size);

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
        _options = options;

        _meter = new Meter("Paprika.Store.PageManager");
        _fileWrites = _meter.CreateHistogram<int>("File writes", "Syscall", "Actual numbers of file writes issued");
        _writeTime = _meter.CreateHistogram<int>("Write time", "ms", "Time spent in writing");

        _prefetcher = Task.Factory.StartNew(RunPrefetcher);
    }

    public static string GetPaprikaFilePath(string dir) => System.IO.Path.Combine(dir, PaprikaFileName);

    public string Path { get; }

    protected override unsafe void* Ptr => _ptr;

    protected override void PrefetchHeavy(DbAddress address) => _prefetches.Writer.TryWrite(address);

    public override void Prefetch(ReadOnlySpan<DbAddress> addresses)
    {
        var writer = _prefetches.Writer;

        foreach (var address in addresses)
        {
            if (address.IsNull == false)
            {
                writer.TryWrite(address);
            }
        }
    }

    private async Task RunPrefetcher()
    {
        var reader = _prefetches.Reader;

        while (await reader.WaitToReadAsync())
        {
            PrefetchImpl(reader, this);
        }

        [SkipLocalsInit]
        static void PrefetchImpl(ChannelReader<DbAddress> reader, MemoryMappedPageManager manager)
        {
            const int maxPrefetch = 128;

            Span<UIntPtr> span = stackalloc UIntPtr[maxPrefetch];
            var i = 0;

            for (; i < maxPrefetch; i++)
            {
                if (reader.TryRead(out var address) == false)
                    break;

                span[i] = manager.GetAt(address).Raw;
            }

            if (i > 0)
            {
                Platform.Prefetch(span[..i], Page.PageSize);
            }
        }
    }

    public override async ValueTask FlushPages(ICollection<DbAddress> dbAddresses, CommitOptions options)
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
            _file.Flush(true);
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
            var addr = span[range.Start];
            _pendingWrites.Add(WriteAt(addr, (uint)range.Length).AsTask());
        }

        _fileWrites.Record(_pendingWrites.Count);
    }

    private ValueTask WriteAt(DbAddress addr, uint count = 1)
    {
        var page = GetAt(addr);
        return RandomAccess.WriteAsync(_file.SafeFileHandle, Own(page, count).Memory, addr.FileOffset);
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

    public override async ValueTask FlushRootPage(DbAddress root, CommitOptions options)
    {
        if (_options == PersistenceOptions.MMapOnly)
            return;

        if (options != CommitOptions.DangerNoWrite)
        {
            await WriteAt(root);
        }

        if (options == CommitOptions.FlushDataAndRoot)
        {
            _file.Flush(true);
        }
    }

    public override void Flush()
    {
        if (_options == PersistenceOptions.MMapOnly)
            return;

        _file.Flush(true);
    }

    public override void ForceFlush()
    {
        _whole.Flush();
        _file.Flush(true);
    }

    public override bool UsesPersistentPaging => _options == PersistenceOptions.FlushFile;

    public override void Dispose()
    {
        _prefetches.Writer.Complete();
        _prefetcher.Wait();

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
