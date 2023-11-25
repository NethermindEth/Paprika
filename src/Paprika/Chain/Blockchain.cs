using System.CodeDom.Compiler;
using System.Diagnostics;
using System.Diagnostics.Metrics;
using System.Runtime.CompilerServices;
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
    private readonly Dictionary<uint, List<BlockState>> _blocksByNumber = new();
    private readonly Dictionary<Keccak, BlockState> _blocksByHash = new();

    private readonly Channel<BlockState> _finalizedChannel;

    // metrics
    private readonly Meter _meter;
    private readonly Histogram<int> _flusherBlockPerS;
    private readonly Histogram<int> _flusherBlockApplicationInMs;
    private readonly Histogram<int> _flusherFlushInMs;
    private readonly Counter<long> _bloomMissedReads;
    private readonly MetricsExtensions.IAtomicIntGauge _flusherQueueCount;

    private readonly PagedDb _db;
    private readonly IPreCommitBehavior _preCommit;
    private readonly TimeSpan _minFlushDelay;
    private readonly Action? _beforeMetricsDisposed;
    private readonly Task _flusher;

    private uint _lastFinalized;

    private static readonly TimeSpan DefaultFlushDelay = TimeSpan.FromSeconds(1);

    public Blockchain(PagedDb db, IPreCommitBehavior preCommit, TimeSpan? minFlushDelay = null,
        int? finalizationQueueLimit = null, Action? beforeMetricsDisposed = null)
    {
        _db = db;
        _preCommit = preCommit;
        _minFlushDelay = minFlushDelay ?? DefaultFlushDelay;
        _beforeMetricsDisposed = beforeMetricsDisposed;

        if (finalizationQueueLimit == null)
        {
            _finalizedChannel = Channel.CreateUnbounded<BlockState>(new UnboundedChannelOptions
            {
                SingleReader = true,
                SingleWriter = true,
            });
        }
        else
        {
            _finalizedChannel = Channel.CreateBounded<BlockState>(
                new BoundedChannelOptions(finalizationQueueLimit.Value)
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

                uint flushedTo = 0;

                while (timer.Elapsed < _minFlushDelay && reader.TryRead(out var block))
                {
                    using var batch = _db.BeginNextBatch();

                    // apply
                    var application = Stopwatch.StartNew();

                    flushed.Add(block.BlockNumber);

                    flushedTo = block.BlockNumber;

                    batch.SetMetadata(block.BlockNumber, block.Hash);

                    block.Apply(batch);

                    // only for debugging if needed
                    //block.Assert(batch);

                    application.Stop();
                    _flusherBlockApplicationInMs.Record((int)application.ElapsedMilliseconds);

                    // commit but no flush here, it's too heavy, the flush will come later
                    await batch.Commit(CommitOptions.DangerNoFlush);

                    // inform blocks about flushing
                    lock (_blockLock)
                    {
                        if (!_blocksByNumber.TryGetValue(flushedTo, out var removedBlocks))
                        {
                            throw new Exception($"Missing blocks at block number {flushedTo}");
                        }

                        var cloned = removedBlocks.ToArray();
                        foreach (var removedBlock in cloned)
                        {
                            // dispose one to allow leases to do the count
                            removedBlock.Dispose();
                        }
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

                Flushed?.Invoke(this, flushedTo);

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

    /// <summary>
    /// Announces the last block number that was flushed to disk.
    /// </summary>
    public event EventHandler<uint> Flushed;

    private void Add(BlockState state)
    {
        // allocate before lock
        var list = new List<BlockState> { state };

        lock (_blockLock)
        {
            // blocks by number first
            ref var blocks =
                ref CollectionsMarshal.GetValueRefOrAddDefault(_blocksByNumber, state.BlockNumber, out var exists);

            if (exists == false)
            {
                blocks = list;
            }
            else
            {
                blocks!.Add(state);
            }

            // blocks by hash
            _blocksByHash.Add(state.Hash, state);
        }
    }

    private void Remove(BlockState blockState)
    {
        lock (_blockLock)
        {
            // blocks by number, use remove first as usually it should be the case
            if (!_blocksByNumber.Remove(blockState.BlockNumber, out var blocks))
            {
                throw new Exception($"Blocks @ {blockState.BlockNumber} should not be empty");
            }

            blocks.Remove(blockState);
            if (blocks.Count > 0)
            {
                // re-add only if not empty
                _blocksByNumber.Add(blockState.BlockNumber, blocks);
            }

            // blocks by hash
            _blocksByHash.Remove(blockState.Hash);
        }
    }

    public IWorldState StartNew(Keccak parentKeccak)
    {
        lock (_blockLock)
        {
            var (batch, ancestors) = BuildBlockDataDependencies(parentKeccak);
            return new BlockState(parentKeccak, batch, ancestors, this);
        }
    }

    public IReadOnlyWorldState StartReadOnly(Keccak parentKeccak)
    {
        lock (_blockLock)
        {
            var (batch, ancestors) = BuildBlockDataDependencies(parentKeccak);
            return new ReadOnlyState(parentKeccak, batch, ancestors);
        }
    }

    public IReadOnlyWorldState StartReadOnlyLatestFromDb()
    {
        var batch = _db.BeginReadOnlyBatch($"Blockchain dependency LATEST");
        return new ReadOnlyState(batch.Metadata.StateHash, batch, Array.Empty<BlockState>());
    }

    private (IReadOnlyBatch batch, BlockState[] ancestors) BuildBlockDataDependencies(Keccak parentKeccak)
    {
        if (_blocksByHash.TryGetValue(parentKeccak, out var ancestor))
        {
            var ancestors = new List<BlockState>();

            do
            {
                ancestor.AcquireLease(); // lease it!
                ancestors.Add(ancestor);
            } while (_blocksByHash.TryGetValue(ancestor.ParentHash, out ancestor));

            // no more ancestors, sort them from the youngest
            var fromYoungest = ancestors.ToArray();
            var oldest = fromYoungest[^1];

            // the easy path, the parent block is in the non-finalized set yet
            var batch = _db.BeginReadOnlyBatch(Normalize(oldest.ParentHash),
                $"Blockchain dependency with parent @ {parentKeccak}");

            return (batch, fromYoungest);
        }
        else
        {
            // the block is in the finalized part, search for it
            var batch = _db.BeginReadOnlyBatch(Normalize(parentKeccak),
                $"Blockchain dependency with parent @ {parentKeccak}");
            return (batch, Array.Empty<BlockState>());
        }

        static Keccak Normalize(in Keccak keccak)
        {
            // pages are zeroed before, return zero on empty tree
            return keccak == Keccak.EmptyTreeHash ? Keccak.Zero : keccak;
        }
    }

    public void Finalize(Keccak keccak)
    {
        Stack<BlockState> finalized;
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

    public bool HasState(in Keccak keccak)
    {
        lock (_blockLock)
        {
            if (_blocksByHash.ContainsKey(keccak))
                return true;

            if (_db.HasState(keccak))
                return true;

            return false;
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
    private class BlockState : RefCountingDisposable, IWorldState, ICommit, IProvideDescription
    {
        /// <summary>
        /// A simple bloom filter to assert whether the given key was set in a given block, used to speed up getting the keys.
        /// </summary>
        private HashSet<ulong>? _bloom;

        /// <summary>
        /// A faster filter constructed on block commit.
        /// </summary>
        private Xor8? _xor;

        /// <summary>
        /// Stores information about contracts that should have their previous incarnations destroyed.
        /// </summary>
        private HashSet<Keccak>? _destroyed;

        private readonly ReadOnlyBatchCountingRefs _batch;
        private BlockState[] _ancestors;

        private readonly Blockchain _blockchain;

        /// <summary>
        /// The maps mapping accounts information, written in this block.
        /// </summary>
        private PooledSpanDictionary _state = null!;

        /// <summary>
        /// The maps mapping storage information, written in this block.
        /// </summary>
        private PooledSpanDictionary _storage = null!;

        /// <summary>
        /// The values set the <see cref="IPreCommitBehavior"/> during the <see cref="ICommit.Visit"/> invocation.
        /// It's both storage & state as it's metadata for the pre-commit behavior.
        /// </summary>
        private PooledSpanDictionary _preCommit = null!;

        private bool _committed;
        private Keccak? _hash;

        public BlockState(Keccak parentStateRoot, IReadOnlyBatch batch, BlockState[] ancestors, Blockchain blockchain)
        {
            _batch = new ReadOnlyBatchCountingRefs(batch);

            _ancestors = ancestors;

            _blockchain = blockchain;

            ParentHash = parentStateRoot;

            // rent pages for the bloom
            _bloom = new HashSet<ulong>();
            _destroyed = null;

            _hash = ParentHash;

            CreateDictionaries();
        }

        private void CreateDictionaries()
        {
            CreateDict(ref _state, Pool);
            CreateDict(ref _storage, Pool);
            CreateDict(ref _preCommit, Pool);
            return;

            // as pre-commit can use parallelism, make the pooled dictionaries concurrent friendly:
            // 1. make the dictionary preserve once written values, which means that it can repeatedly read and set without worrying of ordering operations
            // 2. set dictionary so that it allows concurrent readers
            static void CreateDict(ref PooledSpanDictionary dict, BufferPool pool)
            {
                // ReSharper disable once ConditionIsAlwaysTrueOrFalseAccordingToNullableAPIContract
                // ReSharper disable once UseNullPropagation
                if (dict != null)
                {
                    // dispose previous
                    dict.Dispose();
                }

                dict = new PooledSpanDictionary(pool, true, true);
            }
        }

        public Keccak ParentHash { get; }

        /// <summary>
        /// Commits the block to the block chain.
        /// </summary>
        public Keccak Commit(uint blockNumber)
        {
            EnsureHash();

            var hash = _hash!.Value;

            if (hash == ParentHash)
            {
                throw new Exception("The same state as the parent is not handled now");
            }

            // After this step, this block requires no batch or ancestors.
            // It just provides data on its own as it was committed.
            // Clean up dependencies here: batch and ancestors.
            _batch.Dispose();

            foreach (var ancestor in _ancestors)
            {
                ancestor.Dispose();
            }

            _ancestors = Array.Empty<BlockState>();

            AcquireLease();
            BlockNumber = blockNumber;

            // create xor bloom
            _xor = new Xor8(_bloom!);
            _bloom = null;

            _blockchain.Add(this);
            _committed = true;

            return hash;
        }

        public void Reset()
        {
            EnsureNotCommitted();

            _hash = ParentHash;
            _bloom.Clear();
            _destroyed = null;

            CreateDictionaries();
        }

        public uint BlockNumber { get; private set; }

        public Keccak Hash
        {
            get
            {
                EnsureHash();
                return _hash!.Value;
            }
        }

        private void EnsureHash()
        {
            if (_hash == null)
            {
                _hash = _blockchain._preCommit.BeforeCommit(this);
            }
        }

        private BufferPool Pool => _blockchain._pool;

        public void DestroyAccount(in Keccak address)
        {
            _hash = null;

            var searched = NibblePath.FromKey(address);

            Destroy(searched, _state);
            Destroy(searched, _storage);
            Destroy(searched, _preCommit);

            _destroyed ??= new HashSet<Keccak>();
            _destroyed.Add(address);
            return;

            static void Destroy(NibblePath searched, PooledSpanDictionary dict)
            {
                foreach (var kvp in dict)
                {
                    Key.ReadFrom(kvp.Key, out var key);
                    if (key.Path.Equals(searched))
                    {
                        dict.Destroy(kvp.Key, GetHash(key));
                    }
                }
            }
        }

        public Span<byte> GetStorage(in Keccak address, in Keccak storage, Span<byte> destination)
        {
            var key = Key.StorageCell(NibblePath.FromKey(address), storage);

            using var owner = Get(key);

            // check the span emptiness
            var data = owner.Span;
            if (data.IsEmpty)
                return Span<byte>.Empty;

            data.CopyTo(destination);
            return destination.Slice(0, data.Length);
        }

        public Account GetAccount(in Keccak address)
        {
            var key = Key.Account(NibblePath.FromKey(address));

            using var owner = Get(key);

            // check the span emptiness
            if (owner.Span.IsEmpty)
                return new Account(0, 0);

            Account.ReadFrom(owner.Span, out var result);
            return result;
        }

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
            // clean precalculated hash
            _hash = null;

            EnsureNotCommitted();

            var hash = GetHash(key);
            _bloom.Add(hash);

            dict.Set(key.WriteTo(stackalloc byte[key.MaxByteLength]), hash, payload);
        }

        private void EnsureNotCommitted()
        {
            if (_committed)
                throw new Exception("This blocks has already been committed");
        }

        private void SetImpl(in Key key, in ReadOnlySpan<byte> payload0, in ReadOnlySpan<byte> payload1,
            PooledSpanDictionary dict)
        {
            var hash = GetHash(key);
            _bloom.Add(hash);

            dict.Set(key.WriteTo(stackalloc byte[key.MaxByteLength]), hash, payload0, payload1);
        }

        ReadOnlySpanOwner<byte> ICommit.Get(scoped in Key key) => Get(key);

        void ICommit.Set(in Key key, in ReadOnlySpan<byte> payload) => SetImpl(key, payload, _preCommit);

        void ICommit.Set(in Key key, in ReadOnlySpan<byte> payload0, in ReadOnlySpan<byte> payload1) =>
            SetImpl(key, payload0, payload1, _preCommit);

        public string Describe()
        {
            var writer = new StringWriter();
            var indented = new IndentedTextWriter(writer);
            indented.Indent = 1;

            writer.WriteLine("State:");
            _state.Describe(indented);

            writer.WriteLine("Storage:");
            _storage.Describe(indented);

            writer.WriteLine("PreCommit:");
            _preCommit.Describe(indented);

            return writer.ToString();
        }

        void ICommit.Visit(CommitAction action, TrieType type)
        {
            var dict = type == TrieType.State ? _state : _storage;

            foreach (var kvp in dict)
            {
                Key.ReadFrom(kvp.Key, out var key);
                action(key, kvp.Value);
            }

            if (type == TrieType.State && _destroyed != null)
            {
                foreach (var destroyed in _destroyed)
                {
                    // clean the deletes
                    action(Key.Account(destroyed), ReadOnlySpan<byte>.Empty);
                }
            }
        }

        IChildCommit ICommit.GetChild() => new ChildCommit(Pool, this);

        class ChildCommit : IChildCommit
        {
            private readonly PooledSpanDictionary _dict;
            private readonly BufferPool _pool;
            private readonly ICommit _parent;

            public ChildCommit(BufferPool pool, ICommit parent)
            {
                _dict = new PooledSpanDictionary(pool, true, false);
                _pool = pool;
                _parent = parent;
            }

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

            public void Set(in Key key, in ReadOnlySpan<byte> payload0, in ReadOnlySpan<byte> payload1)
            {
                var hash = GetHash(key);
                var keyWritten = key.WriteTo(stackalloc byte[key.MaxByteLength]);

                _dict.Set(keyWritten, hash, payload0, payload1);
            }

            public void Commit()
            {
                foreach (var kvp in _dict)
                {
                    Key.ReadFrom(kvp.Key, out var key);
                    _parent.Set(key, kvp.Value);
                }
            }

            public IChildCommit GetChild() => new ChildCommit(_pool, this);

            public void Dispose() => _dict.Dispose();
            public override string ToString() => _dict.ToString();
        }

        private ReadOnlySpanOwner<byte> Get(scoped in Key key)
        {
            Debug.Assert(_committed == false,
                "The block is committed and it cleaned up some of its dependencies. It cannot provide data for Get method");

            var hash = GetHash(key);
            var keyWritten = key.WriteTo(stackalloc byte[key.MaxByteLength]);

            var result = TryGet(key, keyWritten, hash, out var succeeded);

            Debug.Assert(succeeded);
            return result;
        }

        /// <summary>
        /// A recursive search through the block and its parent until null is found at the end of the weekly referenced
        /// chain.
        /// </summary>
        private ReadOnlySpanOwner<byte> TryGet(scoped in Key key, scoped ReadOnlySpan<byte> keyWritten, ulong bloom,
            out bool succeeded)
        {
            var owner = TryGetLocal(key, keyWritten, bloom, out succeeded);
            if (succeeded)
                return owner;

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

            // report as succeeded operation. The value is not there but it was walked through.
            succeeded = true;
            return default;
        }

        /// <summary>
        /// Tries to get the key only from this block, acquiring no lease as it assumes that the lease is taken.
        /// </summary>
        public ReadOnlySpanOwner<byte> TryGetLocal(scoped in Key key, scoped ReadOnlySpan<byte> keyWritten,
            ulong bloom, out bool succeeded)
        {
            var mayHave = _committed ? _xor!.MayContain(bloom) : _bloom!.Contains(bloom);

            // check if the change is in the block
            if (!mayHave)
            {
                // if destroyed, return false as no previous one will contain it
                if (IsAccountDestroyed(key))
                {
                    succeeded = true;
                    return default;
                }

                succeeded = false;
                return default;
            }

            // select the map to search for 
            var dict = key.Type switch
            {
                DataType.Account => _state,
                DataType.StorageCell => _storage,
                _ => null
            };

            // first always try pre-commit as it may overwrite data
            if (_preCommit.TryGet(keyWritten, bloom, out var span))
            {
                // return with owned lease
                succeeded = true;
                AcquireLease();
                return new ReadOnlySpanOwner<byte>(span, this);
            }

            if (dict != null && dict.TryGet(keyWritten, bloom, out span))
            {
                // return with owned lease
                succeeded = true;
                AcquireLease();
                return new ReadOnlySpanOwner<byte>(span, this);
            }

            _blockchain._bloomMissedReads.Add(1);

            // if destroyed, return false as no previous one will contain it
            if (IsAccountDestroyed(key))
            {
                succeeded = true;
                return default;
            }

            succeeded = false;
            return default;
        }

        private bool IsAccountDestroyed(scoped in Key key)
        {
            if (_destroyed == null)
                return false;

            if (key.Path.Length != NibblePath.KeccakNibbleCount)
                return false;

            // it's either Account, Storage, or Merkle that is a storage
            return _destroyed.Contains(key.Path.UnsafeAsKeccak);
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
            if (_destroyed is { Count: > 0 })
            {
                foreach (var account in _destroyed)
                {
                    batch.Destroy(NibblePath.FromKey(account));
                }
            }

            Apply(batch, _state);
            Apply(batch, _storage);
            Apply(batch, _preCommit);
        }

        private void Apply(IBatch batch, PooledSpanDictionary dict)
        {
            var preCommit = _blockchain._preCommit;

            foreach (var kvp in dict)
            {
                Key.ReadFrom(kvp.Key, out var key);
                var data = preCommit == null ? kvp.Value : preCommit.InspectBeforeApply(key, kvp.Value);
                batch.SetRaw(key, data);
            }
        }

        public void Assert(IReadOnlyBatch batch)
        {
            using var squashed = new PooledSpanDictionary(Pool);

            Squash(_state, squashed);
            Squash(_storage, squashed);
            Squash(_preCommit, squashed);

            foreach (var kvp in squashed)
            {
                Key.ReadFrom(kvp.Key, out var key);

                if (!batch.TryGet(key, out var value))
                {
                    throw new KeyNotFoundException($"Key {key.ToString()} not found.");
                }

                if (!value.SequenceEqual(kvp.Value))
                {
                    var expected = kvp.Value.ToHexString(false);
                    var actual = value.ToHexString(false);

                    throw new Exception($"Values are different for {key.ToString()}. " +
                                        $"Expected is {expected} while found is {actual}.");
                }
            }
        }

        private static void Squash(PooledSpanDictionary source, PooledSpanDictionary destination)
        {
            Span<byte> span = stackalloc byte[128];

            foreach (var kvp in source)
            {
                Key.ReadFrom(kvp.Key, out var key);
                destination.Set(key.WriteTo(span), GetHash(key), kvp.Value);
            }
        }

        public override string ToString() =>
            base.ToString() + ", " +
            $"{nameof(BlockNumber)}: {BlockNumber}, " +
            $"State: {_state}, " +
            $"Storage: {_storage}, " +
            $"PreCommit: {_preCommit}";
    }

    /// <summary>
    /// Represents a block that is a result of ExecutionPayload.
    /// </summary>
    private class ReadOnlyState : RefCountingDisposable, IReadOnlyWorldState
    {
        private readonly ReadOnlyBatchCountingRefs _batch;
        private readonly BlockState[] _ancestors;

        public ReadOnlyState(Keccak stateRoot, IReadOnlyBatch batch, BlockState[] ancestors)
        {
            _batch = new ReadOnlyBatchCountingRefs(batch);
            _ancestors = ancestors;
            Hash = stateRoot;
        }

        public uint BlockNumber { get; private set; }

        public Keccak Hash { get; }

        public Span<byte> GetStorage(in Keccak address, in Keccak storage, Span<byte> destination)
        {
            var key = Key.StorageCell(NibblePath.FromKey(address), storage);

            using var owner = Get(key);

            // check the span emptiness
            var data = owner.Span;
            if (data.IsEmpty)
                return Span<byte>.Empty;

            data.CopyTo(destination);
            return destination.Slice(0, data.Length);
        }

        public Account GetAccount(in Keccak address)
        {
            var key = Key.Account(NibblePath.FromKey(address));

            using var owner = Get(key);

            // check the span emptiness
            if (owner.Span.IsEmpty)
                return new Account(0, 0);

            Account.ReadFrom(owner.Span, out var result);
            return result;
        }


        public ReadOnlySpanOwner<byte> Get(scoped in Key key)
        {
            var hash = GetHash(key);
            var keyWritten = key.WriteTo(stackalloc byte[key.MaxByteLength]);

            var result = TryGet(key, keyWritten, hash, out var succeeded);

            Debug.Assert(succeeded);
            return result;
        }

        /// <summary>
        /// A recursive search through the block and its parent until null is found at the end of the weekly referenced
        /// chain.
        /// </summary>
        private ReadOnlySpanOwner<byte> TryGet(scoped in Key key, scoped ReadOnlySpan<byte> keyWritten, ulong bloom,
            out bool succeeded)
        {
            // walk all the blocks locally
            foreach (var ancestor in _ancestors)
            {
                var owner = ancestor.TryGetLocal(key, keyWritten, bloom, out succeeded);
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

            // report as succeeded operation. The value is not there but it was walked through.
            succeeded = true;
            return default;
        }

        protected override void CleanUp()
        {
            _batch.Dispose();

            // release all the ancestors
            foreach (var ancestor in _ancestors)
            {
                ancestor.Dispose();
            }
        }

        public override string ToString() =>
            base.ToString() + ", " +
            $"{nameof(BlockNumber)}: {BlockNumber}";
    }

    private static ulong GetHash(in Key key)
    {
        return unchecked((uint)key.GetHashCode());
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