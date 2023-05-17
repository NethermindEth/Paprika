using System.Buffers;
using System.Collections.Concurrent;
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

    private static readonly DbAddress ForceFlushMarker = new(uint.MaxValue - 1);
    private static readonly DbAddress EndOfBatch = new(uint.MaxValue - 2);

    private readonly ConcurrentQueue<DbAddress> _toFlush = new();
    private uint _currentlyFlushedBatchId;
    private volatile uint _lastFlushedBatchId;
    private readonly object _lastFlushedMonitor = new();

    private readonly CancellationTokenSource _cts = new();
    private readonly Task<Task> _flusher;

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

        _flusher = Task.Factory.StartNew(() => RunFlusher(), _cts.Token, TaskCreationOptions.LongRunning,
            TaskScheduler.Default);
    }

    public string Path { get; }

    protected override unsafe void* Ptr => _ptr;

    public override void FlushPages(IReadOnlyCollection<DbAddress> dbAddresses, CommitOptions options)
    {
        // TODO: remove alloc
        var addresses = dbAddresses.ToArray();
        Array.Sort(addresses, (a, b) => a.Raw.CompareTo(b.Raw));

        foreach (var address in addresses)
        {
            _toFlush.Enqueue(address);
        }

        if (options != CommitOptions.DangerNoFlush)
        {
            _toFlush.Enqueue(ForceFlushMarker);
        }
    }

    private async Task RunFlusher()
    {
        var handle = _file.SafeFileHandle;
        const int maxPendingWrites = 4096;

        while (_cts.IsCancellationRequested == false)
        {
            while (_toFlush.TryDequeue(out var addr))
            {
                if (addr == ForceFlushMarker)
                {
                    await AwaitWrites();
                    _file.Flush(true);
                }
                else if (addr == EndOfBatch)
                {
                    _lastFlushedBatchId = _currentlyFlushedBatchId;

                    // notify waiters
                    lock (_lastFlushedMonitor) Monitor.PulseAll(_lastFlushedMonitor);

                }
                else
                {
                    // a regular address to write
                    var offset = addr.Raw * Page.PageSize;
                    var page = GetAt(addr);

                    _currentlyFlushedBatchId = page.Header.BatchId;

                    _pendingWrites.Add(RandomAccess.WriteAsync(handle, Own(page).Memory, offset).AsTask());

                    // TODO: this should be throttling
                    if (_pendingWrites.Count > maxPendingWrites)
                    {
                        await AwaitWrites();
                    }
                }
            }

            await AwaitWrites();

            if (_toFlush.IsEmpty)
            {
                // still empty, wait
                await Task.Delay(50);
            }
        }
    }

    private async Task AwaitWrites()
    {
        await Task.WhenAll(_pendingWrites);
        ReleaseOwners();
        _pendingWrites.Clear();
    }

    public override Page GetAtForWriting(DbAddress address, bool reused)
    {
        var page = GetAt(address);

        if (reused == false)
        {
            return page;
        }

        // the page was reused, need to check whether it was flushed already
        var writtenAt = page.Header.BatchId;

        while (writtenAt > _lastFlushedBatchId)
        {
            // not flushed, wait
            lock (_lastFlushedMonitor)
            {
                Monitor.Wait(_lastFlushedMonitor, TimeSpan.FromMilliseconds(100));
            }
        }

        return page;
    }

    public override void FlushRootPage(DbAddress root, CommitOptions options)
    {
        _toFlush.Enqueue(root);

        if (options == CommitOptions.FlushDataAndRoot)
        {
            _toFlush.Enqueue(ForceFlushMarker);
        }

        _toFlush.Enqueue(EndOfBatch);
    }

    public override void Dispose()
    {
        _cts.Cancel();
        _flusher.GetAwaiter().GetResult();

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