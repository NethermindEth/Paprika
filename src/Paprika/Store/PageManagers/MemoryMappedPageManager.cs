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
    private readonly List<DbAddress> _pendingAddresses = new();

    private static readonly DbAddress FlushMarker = new(uint.MaxValue - 1);

    private readonly ConcurrentQueue<DbAddress> _toFlush = new();
    private readonly HashSet<DbAddress> _beingFlushed = new();
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

        // report as being flushed first
        lock (_beingFlushed)
        {
            foreach (var address in addresses)
            {
                _beingFlushed.Add(address);
            }
        }

        foreach (var address in addresses)
        {
            _toFlush.Enqueue(address);
        }

        if (options != CommitOptions.DangerNoFlush)
        {
            _toFlush.Enqueue(FlushMarker);
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
                if (addr == FlushMarker)
                {
                    await AwaitWrites();
                    _file.Flush(true);
                }
                else
                {
                    // a regular address to write
                    _pendingAddresses.Add(addr);
                    var offset = addr.Raw * Page.PageSize;
                    _pendingWrites.Add(RandomAccess.WriteAsync(handle, Own(GetAt(addr)).Memory, offset).AsTask());

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

        lock (_beingFlushed)
        {
            foreach (var address in _pendingAddresses)
            {
                _beingFlushed.Remove(address);
            }

            // notify waiters
            Monitor.PulseAll(_beingFlushed);
        }

        _pendingWrites.Clear();
        _pendingAddresses.Clear();
    }

    public override Page GetAtForWriting(DbAddress address)
    {
        // ensure that the page is not being flushed atm.
        lock (_beingFlushed)
        {
            while (_beingFlushed.Contains(address))
            {
                Monitor.Wait(_beingFlushed);
            }
        }

        return GetAt(address);
    }

    public override void FlushRootPage(DbAddress root, CommitOptions options)
    {
        lock (_beingFlushed)
        {
            _beingFlushed.Add(root);
        }

        _toFlush.Enqueue(root);

        if (options == CommitOptions.FlushDataAndRoot)
        {
            _toFlush.Enqueue(FlushMarker);
        }
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