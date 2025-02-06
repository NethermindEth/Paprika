using System.Collections.Concurrent;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Threading.Channels;
using Paprika.Chain;
using Paprika.Crypto;
using Paprika.Data;
using Paprika.Utils;

namespace Paprika.Store;

public sealed partial class PagedDb
{
    public IMultiHeadChain OpenMultiHeadChain(int automaticallyFinalizeAfter = int.MaxValue)
    {
        lock (_batchLock)
        {
            if (_batchCurrent != null)
            {
                ThrowOnlyOneBatch();
            }

            var chain = new MultiHeadChain(this, automaticallyFinalizeAfter);
            _batchCurrent = chain;

            return chain;
        }
    }

    private class MultiHeadChain : IMultiHeadChain
    {
        private readonly PagedDb _db;
        private readonly BufferPool _pool;

        // Batches grouped by id and number
        private readonly Dictionary<uint, List<ProposedBatch>> _proposedBatchesByBatchId = new();
        private readonly Dictionary<Keccak, ProposedBatch> _proposedBatchesByHash = new();

        // Readers
        private readonly Dictionary<Keccak, Reader> _readers = new();
        private readonly Queue<Reader> _readersDisposalQueue = new();
        private Reader _lastFinalized;
        private readonly ReaderWriterLockSlim _readerLock = new();

        // Proposed batches that are finalized
        private readonly HashSet<Keccak> _beingFinalized = new();

        private readonly Channel<(ProposedBatch[] batches, TaskCompletionSource tcs)> _finalizationQueue =
            Channel.CreateUnbounded<(ProposedBatch[] batches, TaskCompletionSource tcs)>(new UnboundedChannelOptions
            {
                AllowSynchronousContinuations = false
            });

        // Flusher
        private readonly Task _flusher;
        private uint _lastCommittedBatch;

        // Metrics
        private readonly MetricsExtensions.IAtomicIntGauge _flusherQueueCount;

        /// <summary>
        /// Announces the last block number that was flushed to disk.
        /// </summary>
        public event EventHandler<(uint blockNumber, Keccak blockHash)> Flushed;

        /// <summary>
        /// The flusher failed.
        /// </summary>
        public event EventHandler<Exception> FlusherFailure;

        public MultiHeadChain(PagedDb db, int automaticallyFinalizeAfter)
        {
            _db = db;
            AutomaticallyFinalizeAfter = automaticallyFinalizeAfter;

            _flusherQueueCount = _db._meter.CreateAtomicObservableGauge("Flusher queue size", "Blocks",
                "The number of the blocks in the flush queue");

            _pool = _db._pool;

            // Snapshot all without the oldest, we want to keep only N-1 readers
            var allWithoutTheOldest = _db.SnapshotAll(withoutOldest: true);

            // Sort from oldest to youngest
            Array.Sort(allWithoutTheOldest, (a, b) => a.BatchId.CompareTo(b.BatchId));

            foreach (var batch in allWithoutTheOldest)
            {
                _lastCommittedBatch = Math.Max(batch.BatchId, _lastCommittedBatch);
                var reader = new Reader(_db, CreateNextRoot([], (ReadOnlyBatch)batch), batch, [], _pool);
                RegisterReader(reader);

                _readersDisposalQueue.Enqueue(reader);
                _lastFinalized = reader;
            }

            _flusher = FlusherTask();
        }

        public int AutomaticallyFinalizeAfter { get; }

        private void BuildAndRegisterReader(Keccak stateRootHash)
        {
            Reader reader;

            // Dependencies and creation must be done under the batch lock
            lock (_db._batchLock)
            {
                var read = BuildDependencies(stateRootHash, out var root, out _, out var proposed);
                reader = new Reader(_db, root, read, proposed, _pool);
            }

            RegisterReader(reader);
        }

        private void RegisterReader(Reader reader)
        {
            Reader? previous = null;

            _readerLock.EnterWriteLock();
            try
            {
                ref var slot =
                    ref CollectionsMarshal.GetValueRefOrAddDefault(_readers, reader.Metadata.StateHash, out var exists);
                if (exists)
                {
                    previous = slot;
                }

                slot = reader;
            }
            finally
            {
                _readerLock.ExitWriteLock();
                previous?.Dispose();
            }
        }

        /// <summary>
        /// Proposes a new batch.
        /// </summary>
        public (uint reusePagesOlderThan, uint lastCommittedBatchId, IReadOnlyBatch read) Propose(IReadOnlyBatch read,
            ProposedBatch proposed)
        {
            var hash = proposed.StateHash;

            lock (_db._batchLock)
            {
                if (_proposedBatchesByHash.TryAdd(hash, proposed))
                {
                    // The ownership
                    proposed.AcquireLease();

                    // Add by number
                    ref var list = ref CollectionsMarshal.GetValueRefOrAddDefault(_proposedBatchesByBatchId,
                        proposed.BatchId, out bool exists);

                    if (exists == false)
                    {
                        list = [proposed];
                    }
                    else
                    {
                        list!.Add(proposed);
                    }
                }
                else
                {
                    if (_proposedBatchesByHash[hash].BatchId != proposed.BatchId)
                    {
                        throw new Exception(
                            $"There is a proposed batch with the same state hash {hash} but with a different batch id");
                    }
                }

                // Handle both reregistration of the same hash/batchid and the addition by disposing the read
                // and moving forward with the creation of the next reader.

                read.Dispose();

                BuildAndRegisterReader(hash);

                var next = _db.BeginReadOnlyBatch($"{nameof(MultiHeadChain)} {nameof(Propose)} dependency");

                var minBatchId = GetMinBatchId(proposed.Root);

                return (minBatchId, _lastCommittedBatch, next);
            }
        }

        public IHead BeginNonCommittable(in Keccak stateHash)
        {
            var emptyRoot = new RootPage(_pool.Rent(true));
            emptyRoot.Data.Metadata = new Metadata(1, stateHash);
            emptyRoot.Data.NextFreePage = new DbAddress((uint)_db.HistoryDepth);

            return new HeadTrackingBatch(_db, null, emptyRoot, 0, EmptyReadOnlyBatch.Instance, [], _pool);
        }

        public bool TryLeaseReader(in Keccak stateHash, out IHeadReader reader)
        {
            _readerLock.EnterReadLock();
            try
            {
                if (!_readers.TryGetValue(stateHash, out var r))
                {
                    reader = default;
                    return false;
                }

                reader = r;
                reader.AcquireLease();

                return true;

            }
            finally
            {
                _readerLock.ExitReadLock();
            }
        }

        public IHeadReader LeaseLatestFinalized()
        {
            _readerLock.EnterReadLock();
            try
            {
                Reader reader = _lastFinalized;
                reader.AcquireLease();
                return reader;
            }
            finally
            {
                _readerLock.ExitReadLock();
            }
        }

        public Task Finalize(Keccak keccak)
        {
            lock (_db._batchLock)
            {
                if (_beingFinalized.Add(keccak) == false)
                {
                    // Already registered for finalization, return
                    return Task.CompletedTask;
                }

                var proposed = FindProposed(keccak);
                if (proposed.BatchId <= _lastCommittedBatch)
                {
                    // Already committed for finalization, return
                    return Task.CompletedTask;
                }

                var toFinalize = new Stack<ProposedBatch>();
                var tcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
                toFinalize.Push(proposed);

                // While not finalized yet, add parents
                var nextBatchId = _lastCommittedBatch + 1;

                while (proposed.BatchId > nextBatchId && _beingFinalized.Add(proposed.ParentHash))
                {
                    proposed = FindProposed(proposed.ParentHash);
                    toFinalize.Push(proposed);
                }

                var batches = toFinalize.ToArray();

                Debug.Assert(batches[0].BatchId >= nextBatchId);
                _finalizationQueue.Writer.TryWrite((batches, tcs));

                return tcs.Task;
            }
        }

        public bool HasState(in Keccak keccak)
        {
            _readerLock.EnterReadLock();
            try
            {
                return _readers.ContainsKey(keccak);
            }
            finally
            {
                _readerLock.ExitReadLock();
            }
        }

        private async Task FlusherTask()
        {
            var reader = _finalizationQueue.Reader;

            while (await reader.WaitToReadAsync())
            {
                while (reader.TryRead(out var toFinalize))
                {
                    try
                    {
                        // No flushing for batches atm.
                        const CommitOptions options = CommitOptions.DangerNoFlush;

                        Debug.Assert(toFinalize.batches[0].BatchId == _lastCommittedBatch + 1);

                        foreach (var batch in toFinalize.batches)
                        {
                            var watch = Stopwatch.StartNew();

                            // Data first
                            await _db._manager.WritePages(batch.Changes, options);

                            // Set new root
                            var newRootPage = _db.SetNewRoot(batch.Root);

                            // report
                            _db.ReportDbSize(GetRootSizeInMb(batch.Root));

                            await _db._manager.WriteRootPage(newRootPage, options);

                            List<ProposedBatch> removed;

                            Reader newReader;
                            lock (_db._batchLock)
                            {
                                _db.CommitNewRoot();
                                watch.Stop();

                                _lastCommittedBatch = batch.BatchId;

                                _proposedBatchesByBatchId.Remove(_lastCommittedBatch, out removed);
                                foreach (var b in removed)
                                {
                                    var hash = b.StateHash;
                                    _proposedBatchesByHash.Remove(hash);
                                    _beingFinalized.Remove(hash);
                                }

                                var read = BuildDependencies(batch.Root.Data.Metadata.StateHash, out var root, out _,
                                    out var proposed);
                                newReader = new Reader(_db, root, read, proposed, _pool);
                            }

                            // Register the new reader and await the disposal of the oldest one.
                            await RegisterNewReaderAfterFinalization(removed, newReader);

                            // Only now dispose the removed as they had their data used above.
                            foreach (var b in removed)
                            {
                                b.Dispose();
                            }

                            _db.ReportCommit(watch.Elapsed);
                            _flusherQueueCount.Set(reader.Count);
                        }

                        if (reader.TryPeek(out _) == false)
                        {
                            // For now, perform sync only if there is no other coming.
                            // TODO: This should take into consideration the actual number of blocks written so far 
                            _db.Flush();
                        }

                        Flushed?.Invoke(this, new ValueTuple<uint, Keccak>());
                        toFinalize.tcs.SetResult();
                    }
                    catch (Exception e)
                    {
                        toFinalize.tcs.SetException(e);
                        FlusherFailure?.Invoke(this, e);
                    }
                }
            }

            // Sync before closing
            _db.Flush();
        }

        private Task RegisterNewReaderAfterFinalization(List<ProposedBatch> removed, Reader newReader)
        {
            var toDispose = new List<Reader>();
            Reader? oldest = null;

            // Update readers
            _readerLock.EnterWriteLock();
            try
            {
                // Remove the previous
                foreach (var b in removed)
                {
                    if (_readers.Remove(b.StateHash, out var headReader))
                    {
                        toDispose.Add(headReader);
                    }
                }

                // Register the new one in the dictionary and in the disposal queue
                _readers[newReader.Metadata.StateHash] = newReader;
                _lastFinalized = newReader;

                _readersDisposalQueue.Enqueue(newReader);
                if (_readersDisposalQueue.Count == _db._historyDepth)
                {
                    // Ensure that we keep only N-1 readers for the history, so that the next spin can copy over to the root.
                    oldest = _readersDisposalQueue.Dequeue();
                    _readers.Remove(oldest.Metadata.StateHash);
                }
            }
            finally
            {
                _readerLock.ExitWriteLock();
            }

            // Dispose outside the lock as they were removed from the readers.
            foreach (var reader in toDispose)
            {
                reader.Dispose();
            }

            if (oldest == null)
                return Task.CompletedTask;

            oldest.Dispose();
            return oldest.CleanedUp;
        }

        private ProposedBatch FindProposed(Keccak keccak)
        {
            return _proposedBatchesByHash.TryGetValue(keccak, out var proposed)
                ? proposed
                : throw new Exception($"No batch with {keccak} was proposed to this chain.");
        }

        public IHead Begin(in Keccak stateHash)
        {
            lock (_db._batchLock)
            {
                var read = BuildDependencies(stateHash, out var root, out var minBatchId, out var proposed);
                return new HeadTrackingBatch(_db, this, root, minBatchId, read, proposed, _db._pool);
            }
        }

        private ReadOnlyBatch BuildDependencies(Keccak stateHash, out RootPage root, out uint minBatchId,
            out ProposedBatch[] proposed)
        {
            Debug.Assert(Monitor.IsEntered(_db._batchLock));

            var hash = stateHash;
            var list = new List<ProposedBatch>();

            // The stateHash that is searched is proposed. We need to construct a list of dependencies.
            while (_proposedBatchesByHash.TryGetValue(hash, out var tail))
            {
                list.Add(tail);
                hash = tail.ParentHash;
            }

            // We want to have the oldest first
            list.Reverse();

            // Take the read by the hash of the last one's parent
            var read = (ReadOnlyBatch)_db.BeginReadOnlyBatch(hash);

            // Select the root by either, selecting the last proposed root or getting the read root if there's no proposed.
            root = CreateNextRoot(CollectionsMarshal.AsSpan(list), read);
            minBatchId = GetMinBatchId(root);
            proposed = list.ToArray();
            return read;
        }

        private uint GetMinBatchId(RootPage currentRoot)
        {
            // Omit readonly transactions from the min batch calculation.
            // As MultiHead copies all the modified pages it's safe as they will overwritten only in the memory.
            // Also, use the proposed.Root as this is the root to analyze.
            return _db.CalculateMinBatchId(currentRoot, true);
        }

        private RootPage CreateNextRoot(ReadOnlySpan<ProposedBatch> proposed, ReadOnlyBatch read) =>
            PagedDb.CreateNextRoot(proposed.Length > 0 ? proposed[^1].Root : read.Root, _pool);

        public async ValueTask DisposeAsync()
        {
            _finalizationQueue.Writer.Complete();
            await _flusher;

            foreach (var (_, reader) in _readers)
            {
                reader.Dispose();
            }

            _readers.Clear();

            foreach (var (_, proposed) in _proposedBatchesByHash)
            {
                proposed.Dispose();
            }

            _db.RemoveBatch(this);
        }
    }

    // TODO: consider replacing the array with unmanaged version based on BufferPool, may allocate 2000 items per block which is ~16kb.
    private sealed class ProposedBatch(
        (DbAddress at, Page page)[] changes,
        RootPage root,
        Keccak parentHash,
        BufferPool pool) : RefCountingDisposable
    {
        public Keccak StateHash => Root.Data.Metadata.StateHash;
        public uint BatchId => Root.Header.BatchId;
        public (DbAddress at, Page page)[] Changes { get; } = changes;
        public RootPage Root { get; } = root;
        public Keccak ParentHash { get; } = parentHash;

        protected override void CleanUp()
        {
            pool.Return(Root.AsPage());

            foreach (var (_, page) in Changes)
            {
                pool.Return(page);
            }
        }
    }

    /// <summary>
    /// Represents a batch that is currently considered a head of a list of promised batches.
    /// This is constantly updated so that there's never a moment when the page table needs a full rebuild.
    /// </summary>
    /// <remarks>
    /// The head batch stores all the written pages in a <see cref="_pageTable"/>, a dictionary mapping an address to a page.
    /// This is a squashed version of all the promised batches and the pages that were written in this batch.
    ///
    /// To check whether it's a historical read-only page or a written one, the header can be checked.
    ///
    /// When committing, filter the page table to find pages that have the same batch id as this one.
    /// </remarks>
    private sealed class HeadTrackingBatch : BatchBase, IHead
    {
        private readonly BufferPool _pool;
        private readonly MultiHeadChain? _chain;

        private readonly Dictionary<DbAddress, Page> _pageTable = new();
        private readonly Dictionary<Page, DbAddress> _pageTableReversed = new();
        private readonly List<(DbAddress at, Page page)> _cowed = new();

        // The page table cache is used to speed up lookups against _pageTable.
        // With 8k entries and 12 bytes used per entry we're at ~96kb for this structure.
        private const int PageTableCacheSize = 8192;
        private const int PageTableCacheSizeMask = PageTableCacheSize - 1;
        private readonly (DbAddress addr, Page page)[] _pageTableCache = new (DbAddress, Page)[PageTableCacheSize];

        private readonly Queue<ProposedBatch> _proposed = new();

        // Current values, shifted with every commit
        private RootPage _root;
        private uint _batchId;
        private uint _reusePagesOlderThanBatchId;
        private IReadOnlyBatch _read;
        public Keccak ParentHash { get; private set; }

        public HeadTrackingBatch(PagedDb db, MultiHeadChain? chain, RootPage root,
            uint reusePagesOlderThanBatchId, IReadOnlyBatch read, IEnumerable<ProposedBatch> proposed,
            BufferPool pool) : base(db)
        {
            _chain = chain;
            _root = root;
            _batchId = root.Header.BatchId;
            ParentHash = root.Data.Metadata.StateHash;

            _pool = pool;
            _reusePagesOlderThanBatchId = reusePagesOlderThanBatchId;
            _read = read;

            foreach (var batch in proposed)
            {
                // As enqueued, acquire leases
                batch.AcquireLease();
                _proposed.Enqueue(batch);

                // TODO: this application could be done in parallel if the dictionaries were concurrent
                // potential optimization ahead
                foreach (var (at, page) in batch.Changes)
                {
                    ref var slot =
                        ref CollectionsMarshal.GetValueRefOrAddDefault(_pageTable, at, out var exists);

                    if (exists == false)
                    {
                        // Does not exist, set and set the reverse mapping.
                        slot = page;
                        SetPageTableCache(at, page);

                        _pageTableReversed[page] = at;
                    }
                    else if (ShouldOverwrite(page, slot))
                    {
                        // Remove previous reverse mapping
                        _pageTableReversed.Remove(slot);

                        // Override slot and set the reverse mapping
                        slot = page;
                        SetPageTableCache(at, page);
                        _pageTableReversed[page] = at;
                    }
                }
            }
        }

        public void Commit(uint blockNumber, in Keccak blockHash)
        {
            if (_chain == null)
            {
                throw new Exception(
                    $"This head cannot be committed. " +
                    $"It was created based on the empty root {Keccak.EmptyTreeHash} " +
                    $"and should not be committed.");
            }

            MemoizeAbandoned();

            // Copy the state hash
            ParentHash = Root.Data.Metadata.StateHash;

            SetMetadata(blockNumber, blockHash);

            // The root ownership is now moved to the proposed batch.
            // The batch is automatically leased by this head. It will be leased by the chain as well.
            var batch = new ProposedBatch(_cowed.ToArray(), Root, ParentHash, Db._pool);

            _cowed.Clear();
            Clear();

            // Create new root before it's proposed.
            _root = CreateNextRoot(Root, _pool);

            // Register proposal
            var (reusePagesOlderThan, lastCommittedBatchId, read) = _chain.Propose(_read, batch);

            // Locally track this proposed
            _proposed.Enqueue(batch);

            // Remove pages to be removed
            // TODO: potentially make parallel, by gathering first keys to be removed and only then remove
            while (_proposed.TryPeek(out var first) && first.Root.Header.BatchId <= lastCommittedBatchId)
            {
                var removed = _proposed.Dequeue();

                foreach (var (at, page) in removed.Changes)
                {
                    if (_pageTable.TryGetValue(at, out var actual) && page.Equals(actual))
                    {
                        _pageTable.Remove(at);
                        _pageTableReversed.Remove(actual);

                        ref var cached = ref GetPageTableCacheSlot(at);
                        if (cached.addr == at && cached.page.Equals(actual))
                        {
                            cached = default;
                        }
                    }
                }

                removed.Dispose();
            }

            // Amend local state so that it respects new
            _reusePagesOlderThanBatchId = reusePagesOlderThan;
            _batchId = _root.Header.BatchId;
            _read = read;

            // Try automatically finalize
            var toFinalize = _proposed.Count - _chain.AutomaticallyFinalizeAfter;  
            if (toFinalize > 0)
            {
                // Finalize this
                var candidate = _proposed.Skip(toFinalize - 1).FirstOrDefault();
                Debug.Assert(candidate != null);
                var hash = candidate.StateHash;
                
                // Offload the finalization call to a separate task. No need to wait on it.
                Task.Run(() => _chain.Finalize(hash));
            }
        }

        public override Page GetAt(DbAddress address) => GetAtImpl(address, false);

        public override Page GetAtForWriting(DbAddress address) => GetAtImpl(address, true);

        public override void Prefetch(DbAddress addr)
        {
        }

        protected override void DisposeImpl()
        {
            _pool.Return(_root.AsPage());
            _read.Dispose();

            // return all copies that were not proposed
            foreach (var (_, page) in _cowed)
            {
                _pool.Return(page);
            }

            // Dispose all proposed blocks that are still held by this.
            while (_proposed.TryDequeue(out var proposed))
            {
                proposed.Dispose();
            }
        }

        public override uint BatchId => _batchId;

        protected override RootPage Root => _root;

        public ref readonly Metadata Metadata => ref _root.Data.Metadata;

        protected override uint ReusePagesOlderThanBatchId => _reusePagesOlderThanBatchId;

        public override DbAddress GetAddress(Page page) =>
            _pageTableReversed.TryGetValue(page, out var addr) ? addr : Db.GetAddress(page);

        private Page GetAtImpl(DbAddress addr, bool write)
        {
            ref var cached = ref GetPageTableCacheSlot(addr);

            if (cached.addr == addr)
            {
                // cached, means it must be in the _pageTable
                // The value exists
                var writtenThisBatch = cached.page.Header.BatchId == BatchId;

                if (!write || writtenThisBatch)
                {
                    return cached.page;
                }
            }

            ref var page = ref CollectionsMarshal.GetValueRefOrNullRef(_pageTable, addr);

            if (Unsafe.IsNullRef(ref page) == false)
            {
                // The value exists
                var writtenThisBatch = page.Header.BatchId == BatchId;

                if (!write || writtenThisBatch)
                {
                    return page;
                }

                // Not written this batch, allocate and copy. Memoize in the slot
                page = CreateInMemoryOverride(addr, page);
                return page;
            }

            // Does not exist, fetch from db
            var fromDb = Db.GetAt(addr);

            // Make copy on write, while return raw from db if a read.
            if (!write)
            {
                return fromDb;
            }

            // The entry did not exist before, create one.
            var copy = CreateInMemoryOverride(addr, default);
            _pageTable[addr] = copy;

            return copy;
        }

        private void SetPageTableCache(DbAddress addr, Page page)
        {
            ref var cached = ref GetPageTableCacheSlot(addr);

            cached.page = page;
            cached.addr = addr;
        }

        private ref (DbAddress addr, Page page) GetPageTableCacheSlot(DbAddress addr) =>
            ref Unsafe.Add(ref MemoryMarshal.GetArrayDataReference(_pageTableCache),
                addr.Raw & PageTableCacheSizeMask);

        /// <summary>
        /// Creates an override of a given address by getting a page from the pull and storing it in the map.
        /// </summary>
        /// <param name="at">The address that requires to provide an override.</param>
        /// <param name="toRemove">The source page to be removed from the reverse mapping.</param>
        /// <returns></returns>
        /// <remarks>
        /// The source page <paramref name="toRemove"/> is used only for removal from the dictionary.
        /// </remarks>
        private Page CreateInMemoryOverride(DbAddress at, Page toRemove)
        {
            var page = _pool.Rent(false);

            // Remove the previous reverse mapping if it exists
            if (toRemove.Raw != UIntPtr.Zero)
            {
                _pageTableReversed.Remove(toRemove);
            }

            // Remember reversed mapping
            _pageTableReversed[page] = at;

            // Remember that it's proposed
            _cowed.Add((at, page));

            // Only set cache on overrides and during creation
            ref var cached = ref GetPageTableCacheSlot(at);
            cached.addr = at;
            cached.page = page;

            return page;
        }
    }

    private sealed class Reader : RefCountingDisposable, IReadOnlyBatchContext, IHeadReader, IThreadPoolWorkItem
    {
        private readonly BufferPool _pool;
        private readonly PagedDb _db;
        private readonly Dictionary<DbAddress, Page> _pageTable = new();

        // Current values, shifted with every commit
        private readonly RootPage _root;
        private readonly IReadOnlyBatch _read;
        private readonly ProposedBatch[] _proposed;
        private volatile bool _ready;

        public Reader(PagedDb db, RootPage root, IReadOnlyBatch read, ProposedBatch[] proposed, BufferPool pool)
        {
            _db = db;
            _root = root;
            BatchId = root.Header.BatchId;

            _pool = pool;
            _read = read;
            _proposed = proposed;

            IdCache = new ConcurrentDictionary<Keccak, uint>();

            foreach (var batch in proposed)
            {
                // As enqueued, acquire leases
                batch.AcquireLease();
            }

            if (proposed.Length == 0)
            {
                _ready = true;
                return;
            }

            if (proposed.Length == 1)
            {
                // Don't run on thread with a single proposed
                foreach (var (at, page) in proposed[0].Changes)
                {
                    _pageTable[at] = page;
                }

                _ready = true;
                return;
            }

            // More than 1 proposed o scan, queue it.
            _ready = false;
            ThreadPool.UnsafeQueueUserWorkItem(this, false);
        }

        public Metadata Metadata => _root.Data.Metadata;

        public bool TryGet(scoped in Key key, out ReadOnlySpan<byte> result)
        {
            EnsureReady();
            return _root.TryGet(key, this, out result);
        }

        private void EnsureReady()
        {
            if (_ready == false)
            {
                SpinWait.SpinUntil(() => _ready);
            }
        }

        public Page GetAt(DbAddress address)
        {
            EnsureReady();
            return _pageTable.TryGetValue(address, out var page) ? page : _db.GetAt(address);
        }

        public void Prefetch(DbAddress addr)
        {
        }

        public uint BatchId { get; }

        public IDictionary<Keccak, uint> IdCache { get; }

        protected override void CleanUp()
        {
            EnsureReady();

            _pool.Return(_root.AsPage());
            _read.Dispose();

            // Dispose all proposed blocks that are still held by this.
            foreach (var batch in _proposed)
            {
                batch.Dispose();
            }
        }

        void IThreadPoolWorkItem.Execute()
        {
            foreach (var batch in _proposed)
            {
                // TODO: this application could be done in parallel if the dictionaries were concurrent
                // potential optimization ahead
                foreach (var (at, page) in batch.Changes)
                {
                    ref var slot =
                        ref CollectionsMarshal.GetValueRefOrAddDefault(_pageTable, at, out var exists);

                    if (exists == false || ShouldOverwrite(page, slot))
                    {
                        slot = page;
                    }
                }
            }

            _ready = true;
        }
    }

    /// <summary>
    /// Decides whether the new page should overwrite the one in the mapping for the given <see cref="DbAddress"/>.
    /// </summary>
    /// <param name="new">The new page that can be applied.</param>
    /// <param name="existing">The existing page.</param>
    /// <returns>Whether the existing should be overwritten.</returns>
    private static bool ShouldOverwrite(Page @new, Page existing) => @new.Header.BatchId > existing.Header.BatchId;
}

public interface IHead : IDataSetter, IDataGetter, IDisposable
{
    /// <summary>
    /// Gets the hash of the previous block. Will be set to a new value when <see cref="Commit"/> is called.
    /// </summary>
    public Keccak ParentHash { get; }

    /// <summary>
    /// Commits the changes applied so far, and moves the head tracker to the next one.
    /// </summary>
    void Commit(uint blockNumber, in Keccak blockHash);
}

/// <summary>
/// Provides accessor to get data from the head as well as <see cref="IRefCountingDisposable"/> management.
/// </summary>
public interface IHeadReader : IDataGetter, IRefCountingDisposable
{
}

public interface IMultiHeadChain : IAsyncDisposable
{
    IHead Begin(in Keccak stateHash);

    IHead BeginNonCommittable(in Keccak stateHash);

    bool TryLeaseReader(in Keccak stateHash, out IHeadReader leasedReader);

    IHeadReader LeaseLatestFinalized();

    /// <summary>
    /// Finalizes the given block and all the blocks before it.
    /// </summary>
    Task Finalize(Keccak keccak);

    bool HasState(in Keccak keccak);

    /// <summary>
    /// Announces the last block number that was flushed to disk.
    /// </summary>
    event EventHandler<(uint blockNumber, Keccak blockHash)> Flushed;

    /// <summary>
    /// The flusher failed.
    /// </summary>
    event EventHandler<Exception> FlusherFailure;
}
