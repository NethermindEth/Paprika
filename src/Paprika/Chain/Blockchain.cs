using System.Collections.Concurrent;
using System.Diagnostics;
using System.Threading.Channels;
using Nethermind.Int256;
using Paprika.Crypto;
using Paprika.Data;
using Paprika.Store;
using Paprika.Utils;

namespace Paprika.Chain;

/// <summary>
/// The blockchain is the main component of Paprika, that can deal with latest, safe and finalized blocks.
///
/// For latest and safe, it uses a notion of block, that allows switching heads, querying from different heads etc.
/// For the finalized blocks, they are queued to a <see cref="Channel"/> that is consumed by a flushing mechanism
/// using the <see cref="PagedDb"/>.
/// </summary>
/// <remarks>
/// The current implementation assumes a single threaded access. For multi-threaded, some adjustments will be required.
/// The following should be covered:
/// 1. reading a state at a given time based on the root. Should never fail.
/// 2. TBD
/// </remarks>
public class Blockchain : IAsyncDisposable
{
    // allocate 1024 pages (4MB) at once
    private readonly PagePool _pool = new(1024);

    // It's unlikely that there will be many blocks per number as it would require the network to be heavily fragmented. 
    private readonly ConcurrentDictionary<uint, Block[]> _blocksByNumber = new();
    private readonly ConcurrentDictionary<Keccak, Block> _blocksByHash = new();
    private readonly Channel<Block> _finalizedChannel;
    private readonly ConcurrentQueue<(IReadOnlyBatch reader, IEnumerable<uint> blockNumbers)> _alreadyFlushedTo;

    private readonly PagedDb _db;
    private uint _lastFinalized;
    private IReadOnlyBatch _dbReader;
    private readonly Task _flusher;

    public Blockchain(PagedDb db)
    {
        _db = db;
        _finalizedChannel = Channel.CreateUnbounded<Block>(new UnboundedChannelOptions
        {
            SingleReader = true,
            SingleWriter = true
        });

        _alreadyFlushedTo = new();
        _dbReader = db.BeginReadOnlyBatch();

        _flusher = FinalizedFlusher();
    }

    /// <summary>
    /// The flusher method run as a reader of the <see cref="_finalizedChannel"/>.
    /// </summary>
    private async Task FinalizedFlusher()
    {
        var reader = _finalizedChannel.Reader;

        while (await reader.WaitToReadAsync())
        {
            // bulk all the finalized blocks in one batch
            List<uint> flushedBlockNumbers = new();

            var watch = Stopwatch.StartNew();

            using var batch = _db.BeginNextBatch();
            while (watch.Elapsed < FlushEvery && reader.TryRead(out var block))
            {
                flushedBlockNumbers.Add(block.BlockNumber);

                //batch.SetMetadata(block.BlockNumber, block.Hash);

                // TODO: flush the block by adding data to it
                // finalizedBlock.
            }

            await batch.Commit(CommitOptions.FlushDataAndRoot);

            _alreadyFlushedTo.Enqueue((_db.BeginReadOnlyBatch(), flushedBlockNumbers));
        }
    }

    private static readonly TimeSpan FlushEvery = TimeSpan.FromSeconds(2);

    public IWorldState StartNew(Keccak parentKeccak, Keccak blockKeccak, uint blockNumber)
    {
        var parent = _blocksByHash.TryGetValue(parentKeccak, out var p) ? p : null;

        // not added to dictionaries until Commit
        return new Block(parentKeccak, parent, blockKeccak, blockNumber, this);
    }

    public void Finalize(Keccak keccak)
    {
        ReuseAlreadyFlushed();

        // find the block to finalize
        if (_blocksByHash.TryGetValue(keccak, out var block) == false)
        {
            throw new Exception("Block that is marked as finalized is not present");
        }

        Debug.Assert(block.BlockNumber > _lastFinalized,
            "Block that is finalized should have a higher number than the last finalized");

        // gather all the blocks between last finalized and this.
        var count = block.BlockNumber - _lastFinalized;
        Stack<Block> finalized = new((int)count);
        for (var blockNumber = block.BlockNumber; blockNumber > _lastFinalized; blockNumber--)
        {
            // to finalize
            finalized.Push(block);

            // move to next
            block = _blocksByHash[block.ParentHash];
        }

        while (finalized.TryPop(out block))
        {
            // publish for the PagedDb
            _finalizedChannel.Writer.TryWrite(block);
        }

        _lastFinalized += count;
    }

    /// <summary>
    /// Finds the given key using the db reader representing the finalized blocks.
    /// </summary>
    private bool TryReadFromFinalized(in Key key, out ReadOnlySpan<byte> result)
    {
        return _dbReader.TryGet(key, out result);
    }

    private void ReuseAlreadyFlushed()
    {
        while (_alreadyFlushedTo.TryDequeue(out var flushed))
        {
            // TODO: this is wrong, non volatile access, no visibility checks. For now should do.

            // set the last reader
            var previous = _dbReader;

            _dbReader = flushed.reader;

            previous.Dispose();

            foreach (var blockNumber in flushed.blockNumbers)
            {
                _lastFinalized = Math.Max(blockNumber, _lastFinalized);

                // clean blocks with a given number
                if (_blocksByNumber.Remove(blockNumber, out var blocks))
                {
                    foreach (var block in blocks)
                    {
                        block.Dispose();
                    }
                }
            }
        }
    }

    /// <summary>
    /// Represents a block that is a result of ExecutionPayload, storing it in a in-memory trie
    /// </summary>
    private class Block : RefCountingDisposable, IBatchContext, IWorldState
    {
        public Keccak Hash { get; }
        public Keccak ParentHash { get; }
        public uint BlockNumber { get; }

        // a weak-ref to allow collecting blocks once they are finalized
        private readonly WeakReference<Block>? _parent;
        private readonly DataPage _root;
        private readonly BloomFilter _bloom;

        private readonly Blockchain _blockchain;

        private readonly List<Page> _pages = new();
        private readonly Dictionary<Page, DbAddress> _page2Address = new();

        public Block(Keccak parentHash, Block? parent, Keccak hash, uint blockNumber, Blockchain blockchain)
        {
            _parent = parent != null ? new WeakReference<Block>(parent) : null;
            _blockchain = blockchain;

            Hash = hash;
            BlockNumber = blockNumber;
            ParentHash = parentHash;

            // rent pages for the bloom
            _bloom = new BloomFilter(GetNewPage(out _, true));

            // rent one page for the root of the data
            _root = new DataPage(GetNewPage(out _, true));
        }

        /// <summary>
        /// Commits the block to the block chain.
        /// </summary>
        public void Commit()
        {
            // set to blocks in number and in blocks by hash
            _blockchain._blocksByNumber.AddOrUpdate(BlockNumber,
                static (_, block) => new[] { block },
                static (_, existing, block) =>
                {
                    var array = existing;
                    Array.Resize(ref array, array.Length + 1);
                    array[^1] = block;
                    return array;
                }, this);

            _blockchain._blocksByHash.TryAdd(Hash, this);
        }

        private PagePool Pool => _blockchain._pool;

        public UInt256 GetStorage(in Keccak account, in Keccak address)
        {
            var bloom = BloomForStorageOperation(account, address);
            var key = Key.StorageCell(NibblePath.FromKey(account), address);

            using var owner = TryGet(bloom, key);
            if (owner.IsEmpty == false)
            {
                Serializer.ReadStorageValue(owner.Span, out var value);
                return value;
            }

            // TODO: memory ownership of the span
            if (_blockchain.TryReadFromFinalized(in key, out var span))
            {
                Serializer.ReadStorageValue(span, out var value);
                return value;
            }

            return default;
        }

        public Account GetAccount(in Keccak account)
        {
            var bloom = BloomForAccountOperation(account);
            var key = Key.Account(NibblePath.FromKey(account));

            using var owner = TryGet(bloom, key);
            if (owner.IsEmpty == false)
            {
                Serializer.ReadAccount(owner.Span, out var result);
                return result;
            }

            // TODO: memory ownership of the span
            if (_blockchain.TryReadFromFinalized(in key, out var span))
            {
                Serializer.ReadAccount(span, out var result);
                return result;
            }

            return default;
        }

        private static int BloomForStorageOperation(in Keccak key, in Keccak address) =>
            key.GetHashCode() ^ address.GetHashCode();

        private static int BloomForAccountOperation(in Keccak key) => key.GetHashCode();

        public void SetAccount(in Keccak key, in Account account)
        {
            _bloom.Set(BloomForAccountOperation(key));

            _root.SetAccount(NibblePath.FromKey(key), account, this);
        }

        public void SetStorage(in Keccak key, in Keccak address, UInt256 value)
        {
            _bloom.Set(BloomForStorageOperation(key, address));

            _root.SetStorage(NibblePath.FromKey(key), address, value, this);
        }

        Page IPageResolver.GetAt(DbAddress address) => _pages[(int)(address.Raw - AddressOffset)];

        uint IReadOnlyBatchContext.BatchId => 0;

        private const uint AddressOffset = 1;

        DbAddress IBatchContext.GetAddress(Page page) => _page2Address[page];

        public Page GetNewPage(out DbAddress addr, bool clear)
        {
            var page = Pool.Rent();

            page.Clear(); // always clear

            _pages.Add(page);

            addr = DbAddress.Page((uint)(_pages.Count + AddressOffset));
            _page2Address[page] = addr;

            return page;
        }

        Page IBatchContext.GetWritableCopy(Page page) =>
            throw new Exception("The COW should never happen in block. It should always use only writable pages");

        /// <summary>
        /// The implementation assumes that all the pages are writable.
        /// </summary>
        bool IBatchContext.WasWritten(DbAddress addr) => true;

        /// <summary>
        /// A recursive search through the block and its parent until null is found at the end of the weekly referenced
        /// chain.
        /// </summary>
        private ReadOnlySpanOwner<byte> TryGet(int bloom, in Key key)
        {
            var acquired = TryAcquireLease();
            if (acquired == false)
            {
                return default;
            }

            // lease: acquired
            if (_bloom.IsSet(bloom))
            {
                if (_root.TryGet(key, this, out var span))
                {
                    // return with owned lease
                    return new ReadOnlySpanOwner<byte>(span, this);
                }
            }

            // lease no longer needed
            ReleaseLeaseOnce();

            // search the parent
            if (_parent == null || !_parent.TryGetTarget(out var parent))
            {
                return default;
            }

            return parent.TryGet(bloom, key);
        }

        protected override void CleanUp()
        {
            // return all the pages
            foreach (var page in _pages)
            {
                Pool.Return(page);
            }
        }
    }

    public ValueTask DisposeAsync()
    {
        _finalizedChannel.Writer.Complete();
        _pool.Dispose();
        return new ValueTask(_flusher);
    }
}