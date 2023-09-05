﻿using System.Diagnostics;
using System.Diagnostics.Metrics;
using System.Runtime.InteropServices;
using System.Threading.Channels;
using Paprika.Crypto;
using Paprika.Data;
using Paprika.Merkle;
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
public class Blockchain : IAsyncDisposable
{
    // allocate 1024 pages (4MB) at once
    private readonly BufferPool _pool = new(1024);

    private readonly object _blockLock = new();
    private readonly Dictionary<uint, List<Block>> _blocksByNumber = new();
    private readonly Dictionary<Keccak, Block> _blocksByHash = new();

    private readonly Channel<Block> _finalizedChannel;

    // metrics
    private readonly Meter _meter;
    private readonly Histogram<int> _flusherBlockPerS;
    private readonly Histogram<int> _flusherBlockApplicationInMs;
    private readonly Histogram<int> _flusherFlushInMs;
    private readonly Counter<long> _bloomMissedReads;
    private readonly MetricsExtensions.IAtomicIntGauge _flusherQueueCount;

    private readonly PagedDb _db;
    private readonly IPreCommitBehavior? _preCommit;
    private readonly TimeSpan _minFlushDelay;
    private readonly Action? _beforeMetricsDisposed;
    private readonly Task _flusher;

    private uint _lastFinalized;

    private static readonly TimeSpan DefaultFlushDelay = TimeSpan.FromSeconds(1);

    public Blockchain(PagedDb db, IPreCommitBehavior? preCommit = null, TimeSpan? minFlushDelay = null,
        int? finalizationQueueLimit = null, Action? beforeMetricsDisposed = null)
    {
        _db = db;
        _preCommit = preCommit;
        _minFlushDelay = minFlushDelay ?? DefaultFlushDelay;
        _beforeMetricsDisposed = beforeMetricsDisposed;

        if (finalizationQueueLimit == null)
        {
            _finalizedChannel = Channel.CreateUnbounded<Block>(new UnboundedChannelOptions
            {
                SingleReader = true,
                SingleWriter = true,
            });
        }
        else
        {
            _finalizedChannel = Channel.CreateBounded<Block>(new BoundedChannelOptions(finalizationQueueLimit.Value)
            {
                SingleReader = true,
                SingleWriter = true,
                FullMode = BoundedChannelFullMode.Wait,
            });
        }

        _flusher = FlusherTask();

        // metrics
        _meter = new Meter("Paprika.Chain.Blockchain");

        _flusherBlockPerS = _meter.CreateHistogram<int>("Blocks stored / s", "Blocks/s",
            "The number of blocks stored by the flushing task in one second");
        _flusherBlockApplicationInMs = _meter.CreateHistogram<int>("Block data application in ms", "ms",
            "The amortized time it takes for one block to apply on PagedDb");
        _flusherFlushInMs = _meter.CreateHistogram<int>("FSYNC time", "ms",
            "The time it took to synchronize the file");
        _flusherQueueCount = _meter.CreateAtomicObservableGauge("Flusher queue size", "Blocks",
            "The number of the blocks in the flush queue");
        _bloomMissedReads = _meter.CreateCounter<long>("Bloom missed reads", "Reads",
            "Number of reads that passed bloom but missed in dictionary");
    }

    /// <summary>
    /// The flusher method run as a reader of the <see cref="_finalizedChannel"/>.
    /// </summary>
    private async Task FlusherTask()
    {
        var reader = _finalizedChannel.Reader;

        try
        {
            while (await reader.WaitToReadAsync())
            {
                var flushed = new List<uint>();
                var timer = Stopwatch.StartNew();

                while (timer.Elapsed < _minFlushDelay && reader.TryRead(out var block))
                {
                    using var batch = _db.BeginNextBatch();

                    // apply
                    var application = Stopwatch.StartNew();

                    flushed.Add(block.BlockNumber);
                    var flushedTo = block.BlockNumber;

                    batch.SetMetadata(block.BlockNumber, block.Hash);

                    block.Apply(batch);

                    application.Stop();
                    _flusherBlockApplicationInMs.Record((int)application.ElapsedMilliseconds);

                    // commit but no flush here, it's too heavy, the flush will come later
                    await batch.Commit(CommitOptions.DangerNoFlush);

                    // inform the blocks about flushing
                    Block[] blocks;

                    lock (_blockLock)
                    {
                        if (!_blocksByNumber.TryGetValue(flushedTo, out var removedBlocks))
                        {
                            throw new Exception($"Missing blocks at block number {flushedTo}");
                        }

                        blocks = removedBlocks.ToArray();

                        // destroy dependencies on previous by setting up the next
                        if (_blocksByNumber.TryGetValue(flushedTo + 1, out var nextBlocks))
                        {
                            foreach (var next in nextBlocks)
                            {
                                var (b, ancestors) = BuildBlockDataDependencies(next.ParentHash, next.BlockNumber);
                                next.ReplaceData(b, ancestors);
                            }
                        }
                    }

                    foreach (var removedBlock in blocks)
                    {
                        // dispose one to allow leases to do the count
                        removedBlock.Dispose();
                    }

                    _flusherQueueCount.Subtract(1);
                }

                timer.Stop();

                // measure
                var count = flushed.Count;

                if (count == 0)
                {
                    // nothing
                    continue;
                }

                var flushWatch = Stopwatch.StartNew();
                _db.Flush();
                _flusherFlushInMs.Record((int)flushWatch.ElapsedMilliseconds);

                if (timer.ElapsedMilliseconds > 0)
                {
                    _flusherBlockPerS.Record((int)(count * 1000 / timer.ElapsedMilliseconds));
                }
            }
        }
        catch (Exception e)
        {
            Console.WriteLine(e);
            throw;
        }
    }

    private void Add(Block block)
    {
        // allocate before lock
        var list = new List<Block> { block };

        lock (_blockLock)
        {
            // blocks by number first
            ref var blocks =
                ref CollectionsMarshal.GetValueRefOrAddDefault(_blocksByNumber, block.BlockNumber,
                    out var exists);

            if (exists == false)
            {
                blocks = list;
            }
            else
            {
                blocks.Add(block);
            }

            // blocks by hash
            _blocksByHash.Add(block.Hash, block);
        }
    }

    private void Remove(Block block)
    {
        lock (_blockLock)
        {
            // blocks by number, use remove first as usually it should be the case
            if (!_blocksByNumber.Remove(block.BlockNumber, out var blocks))
            {
                throw new Exception($"Blocks @ {block.BlockNumber} should not be empty");
            }

            blocks.Remove(block);
            if (blocks.Count > 0)
            {
                // re-add only if not empty
                _blocksByNumber.Add(block.BlockNumber, blocks);
            }

            // blocks by hash
            _blocksByHash.Remove(block.Hash);
        }
    }

    public IWorldState StartNew(Keccak parentKeccak, Keccak blockKeccak, uint blockNumber)
    {
        lock (_blockLock)
        {
            var (batch, ancestors) = BuildBlockDataDependencies(parentKeccak, blockNumber);
            return new Block(parentKeccak, blockKeccak, blockNumber, batch, ancestors, this);
        }
    }

    private (IReadOnlyBatch batch, Block[] ancestors) BuildBlockDataDependencies(Keccak parentKeccak, uint blockNumber)
    {
        var batch = _db.BeginReadOnlyBatch($"{nameof(Blockchain)}.{nameof(StartNew)} @ {blockNumber}");
        var batchBlockNumber = batch.Metadata.BlockNumber;
        var blocksToRead = blockNumber - batchBlockNumber - 1;

        var ancestors = new Block[blocksToRead];

        var parent = parentKeccak;

        for (var i = 0; i < blocksToRead; i++)
        {
            if (_blocksByHash.TryGetValue(parent, out var parentBlock) == false)
            {
                throw new Exception($"Missing block: @{blockNumber - 1}");
            }

            // lease parent
            parentBlock.AcquireLease();

            ancestors[i] = parentBlock;
            parent = parentBlock.ParentHash;
        }

        return (batch, ancestors);
    }

    public void Finalize(Keccak keccak)
    {
        Stack<Block> finalized;
        uint count;

        // gather all the blocks to finalize 
        lock (_blockLock)
        {
            if (_blocksByHash.TryGetValue(keccak, out var block) == false)
            {
                throw new Exception("Block that is marked as finalized is not present");
            }

            Debug.Assert(block.BlockNumber > _lastFinalized,
                "Block that is finalized should have a higher number than the last finalized");

            // gather all the blocks between last finalized and this.

            count = block.BlockNumber - _lastFinalized;

            finalized = new((int)count);
            for (var blockNumber = block.BlockNumber; blockNumber > _lastFinalized; blockNumber--)
            {
                // no need to acquire lease here, the block is already leased for the blockchain before Add(block)
                finalized.Push(block);
                if (_blocksByHash.TryGetValue(block.ParentHash, out block) == false)
                {
                    break;
                }
            }

            _lastFinalized += count;
        }

        // report count before actual writing to do no
        _flusherQueueCount.Add((int)count);

        // push them!
        var writer = _finalizedChannel.Writer;

        while (finalized.TryPop(out var block))
        {
            if (writer.TryWrite(block) == false)
            {
                // hard spin wait on breaching the size
                SpinWait.SpinUntil(() => writer.TryWrite(block));
            }
        }
    }

    private class ReadOnlyBatchCountingRefs : RefCountingDisposable, IReadOnlyBatch
    {
        private readonly IReadOnlyBatch _batch;

        public ReadOnlyBatchCountingRefs(IReadOnlyBatch batch)
        {
            _batch = batch;
            Metadata = batch.Metadata;
            BatchId = batch.BatchId;
        }

        protected override void CleanUp() => _batch.Dispose();

        public Metadata Metadata { get; }

        public uint BatchId { get; }


        public bool TryGet(scoped in Key key, out ReadOnlySpan<byte> result) => _batch.TryGet(key, out result);

        public void Report(IReporter reporter) =>
            throw new NotImplementedException("One should not report over a block");

        public override string ToString() => base.ToString() + $", BatchId:{_batch.BatchId}";
    }

    /// <summary>
    /// Represents a block that is a result of ExecutionPayload.
    /// </summary>
    private class Block : RefCountingDisposable, IWorldState, ICommit
    {
        public Keccak Hash { get; }
        public Keccak ParentHash { get; }
        public uint BlockNumber { get; }

        /// <summary>
        /// A simple bloom filter to assert whether the given key was set in a given block,
        /// used to speed up getting the keys.
        /// </summary>
        private readonly HashSet<int> _bloom;

        private ReadOnlyBatchCountingRefs _batch;
        private Block[] _ancestors;
        private readonly ReaderWriterLockSlim _lock;

        private readonly Blockchain _blockchain;

        /// <summary>
        /// The maps mapping accounts information, written in this block.
        /// </summary>
        private readonly PooledSpanDictionary _state;

        /// <summary>
        /// The maps mapping storage information, written in this block.
        /// </summary>
        private readonly PooledSpanDictionary _storage;

        /// <summary>
        /// The values set the <see cref="IPreCommitBehavior"/> during the <see cref="ICommit.Visit"/> invocation.
        /// It's both storage & state as it's metadata for the pre-commit behavior.
        /// </summary>
        private readonly PooledSpanDictionary _preCommit;

        private bool _committed;

        public Block(Keccak parentHash, Keccak hash, uint blockNumber, IReadOnlyBatch batch, Block[] ancestors,
            Blockchain blockchain)
        {
            _batch = new ReadOnlyBatchCountingRefs(batch);
            _lock = new ReaderWriterLockSlim();

            _ancestors = ancestors;

            _blockchain = blockchain;

            Hash = hash;
            BlockNumber = blockNumber;
            ParentHash = parentHash;

            // rent pages for the bloom
            _bloom = new HashSet<int>();

            _state = new PooledSpanDictionary(Pool);
            _storage = new PooledSpanDictionary(Pool);
            _preCommit = new PooledSpanDictionary(Pool);
        }

        /// <summary>
        /// Commits the block to the block chain.
        /// </summary>
        public object Commit()
        {
            // run pre-commit
            var result = _blockchain._preCommit?.BeforeCommit(this);

            AcquireLease();
            _blockchain.Add(this);
            _committed = true;

            return result ?? "null";
        }

        private BufferPool Pool => _blockchain._pool;

        public byte[] GetStorage(in Keccak address, in Keccak storage)
        {
            var key = Key.StorageCell(NibblePath.FromKey(address), storage);

            using var owner = Get(key);

            // check the span emptiness
            if (owner.Span.IsEmpty)
                return Array.Empty<byte>();

            return owner.Span.ToArray();
        }

        public Account GetAccount(in Keccak address)
        {
            var key = Key.Account(NibblePath.FromKey(address));

            using var owner = Get(key);

            // check the span emptiness
            if (owner.Span.IsEmpty)
                return default;

            Account.ReadFrom(owner.Span, out var result);
            return result;
        }

        private static int GetHash(in Key key) => key.GetHashCode();

        public void SetAccount(in Keccak address, in Account account)
        {
            var path = NibblePath.FromKey(address);
            var key = Key.Account(path);

            var payload = account.WriteTo(stackalloc byte[Account.MaxByteCount]);

            SetImpl(key, payload, _state);
        }

        public void SetStorage(in Keccak address, in Keccak storage, ReadOnlySpan<byte> value)
        {
            var path = NibblePath.FromKey(address);
            var key = Key.StorageCell(path, storage);

            SetImpl(key, value, _storage);
        }

        private void SetImpl(in Key key, in ReadOnlySpan<byte> payload, PooledSpanDictionary dict)
        {
            var hash = GetHash(key);
            _bloom.Add(hash);

            dict.Set(key.WriteTo(stackalloc byte[key.MaxByteLength]), hash, payload);
        }

        ReadOnlySpanOwner<byte> ICommit.Get(scoped in Key key) => Get(key);

        void ICommit.Set(in Key key, in ReadOnlySpan<byte> payload) => SetImpl(key, payload, _preCommit);

        void ICommit.Visit(CommitAction action, TrieType type)
        {
            var dict = type == TrieType.State ? _state : _storage;

            foreach (var kvp in dict)
            {
                Key.ReadFrom(kvp.Key, out var key);
                action(key, kvp.Value);
            }
        }

        IChildCommit ICommit.GetChild() => new ChildCommit(new PooledSpanDictionary(Pool), this);

        class ChildCommit : IChildCommit
        {
            private readonly PooledSpanDictionary _dict;
            private readonly ICommit _parent;

            public ChildCommit(PooledSpanDictionary dictionary, ICommit parent)
            {
                _dict = dictionary;
                _parent = parent;
            }

            public void Dispose() => _dict.Dispose();

            public ReadOnlySpanOwner<byte> Get(scoped in Key key)
            {
                var hash = GetHash(key);
                var keyWritten = key.WriteTo(stackalloc byte[key.MaxByteLength]);

                if (_dict.TryGet(keyWritten, hash, out var result))
                {
                    return new ReadOnlySpanOwner<byte>(result, null);
                }

                return _parent.Get(key);
            }

            public void Set(in Key key, in ReadOnlySpan<byte> payload)
            {
                var hash = GetHash(key);
                var keyWritten = key.WriteTo(stackalloc byte[key.MaxByteLength]);

                _dict.Set(keyWritten, hash, payload);
            }

            public void Commit()
            {
                foreach (var kvp in _dict)
                {
                    Key.ReadFrom(kvp.Key, out var key);
                    _parent.Set(key, kvp.Value);
                }
            }

            public override string ToString() => _dict.ToString();
        }

        private ReadOnlySpanOwner<byte> Get(scoped in Key key)
        {
            var hash = GetHash(key);
            var keyWritten = key.WriteTo(stackalloc byte[key.MaxByteLength]);

            var result = TryGet(key, keyWritten, hash, out var succeeded);
            if (succeeded)
                return result;

            // slow path
            while (true)
            {
                result = TryGet(key, keyWritten, hash, out succeeded);
                if (succeeded)
                    return result;

                Thread.Yield();
            }
        }

        /// <summary>
        /// A recursive search through the block and its parent until null is found at the end of the weekly referenced
        /// chain.
        /// </summary>
        private ReadOnlySpanOwner<byte> TryGet(scoped in Key key, scoped ReadOnlySpan<byte> keyWritten, int bloom,
            out bool succeeded)
        {
            var owner = TryGetLocal(key, keyWritten, bloom, out succeeded);
            if (succeeded)
                return owner;

            _lock.EnterReadLock();

            try
            {
                // walk all the blocks locally
                foreach (var ancestor in _ancestors)
                {
                    owner = ancestor.TryGetLocal(key, keyWritten, bloom, out succeeded);
                    if (succeeded)
                        return owner;
                }

                if (_batch.TryGet(key, out var span))
                {
                    // return leased batch
                    succeeded = true;
                    _batch.AcquireLease();
                    return new ReadOnlySpanOwner<byte>(span, _batch);
                }
            }
            finally
            {
                _lock.ExitReadLock();
            }

            // report as succeeded operation. The value is not there but it was walked through.
            succeeded = true;
            return default;
        }

        public void ReplaceData(IReadOnlyBatch batch, Block[] ancestors)
        {
            ReadOnlyBatchCountingRefs prevBatch;
            Block[] prevAncestors;

            _lock.EnterWriteLock();
            try
            {
                // swap previous
                prevBatch = _batch;
                prevAncestors = _ancestors;

                _batch = new ReadOnlyBatchCountingRefs(batch);
                _ancestors = ancestors;
            }
            finally
            {
                _lock.ExitWriteLock();
            }

            // release previous
            prevBatch.Dispose();
            foreach (var ancestor in prevAncestors)
            {
                ancestor.Dispose();
            }
        }

        /// <summary>
        /// Tries to get the key only from this block, acquiring no lease as it assumes that the lease is taken.
        /// </summary>
        private ReadOnlySpanOwner<byte> TryGetLocal(scoped in Key key, scoped ReadOnlySpan<byte> keyWritten,
            int bloom, out bool succeeded)
        {
            if (!_bloom.Contains(bloom))
            {
                succeeded = false;
                return default;
            }

            // select the map to search for 
            var dict = key.Type switch
            {
                DataType.Account => _state,
                DataType.StorageCell => _storage,
                _ => _preCommit
            };

            if (dict.TryGet(keyWritten, bloom, out var span))
            {
                // return with owned lease
                succeeded = true;

                AcquireLease();

                return new ReadOnlySpanOwner<byte>(span, this);
            }

            _blockchain._bloomMissedReads.Add(1);

            succeeded = false;
            return default;
        }

        protected override void CleanUp()
        {
            _state.Dispose();
            _storage.Dispose();
            _preCommit.Dispose();

            _batch.Dispose();

            // release all the ancestors
            foreach (var ancestor in _ancestors)
            {
                ancestor.Dispose();
            }

            if (_committed)
            {
                _blockchain.Remove(this);
            }
        }

        public void Apply(IBatch batch)
        {
            Apply(batch, _state);
            Apply(batch, _storage);
            Apply(batch, _preCommit);
        }

        private static void Apply(IBatch batch, PooledSpanDictionary dict)
        {
            foreach (var kvp in dict)
            {
                Key.ReadFrom(kvp.Key, out var key);
                batch.SetRaw(key, kvp.Value);
            }
        }

        public override string ToString() =>
            base.ToString() + ", " +
            $"{nameof(BlockNumber)}: {BlockNumber}, " +
            $"State: {_state}, " +
            $"Storage: {_storage}, " +
            $"PreCommit: {_preCommit}";
    }

    public async ValueTask DisposeAsync()
    {
        // mark writer as complete
        _finalizedChannel.Writer.Complete();

        // await the flushing task
        await _flusher;

        // dispose all memoized blocks to please the ref-counting
        foreach (var (_, block) in _blocksByHash)
        {
            block.Dispose();
        }

        _blocksByHash.Clear();
        _blocksByNumber.Clear();

        // once the flushing is done and blocks are disposed, dispose the pool
        _pool.Dispose();

        Debugger.Log(0, null, $"Next free page: {_db.NextFreePage}\n\n");

        // dispose metrics, but flush them last time before unregistering
        _beforeMetricsDisposed?.Invoke();
        _meter.Dispose();
    }
}