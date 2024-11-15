﻿using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Paprika.Chain;
using Paprika.Crypto;
using Paprika.Utils;

namespace Paprika.Store;

public sealed partial class PagedDb
{
    public IMultiHeadChain OpenMultiHeadChain()
    {
        // TODO: properly set and mark db as not capable of using batches.
        return new MultiMultiHeadChain(this, int.MaxValue);
    }

    private class MultiMultiHeadChain(PagedDb db, int maxDepth) : IMultiHeadChain
    {
        private readonly Dictionary<uint, List<ProposedBatch>> _proposedBatchesByBatchId = new();
        private readonly Dictionary<Keccak, ProposedBatch> _proposedBatchesByHash = new();
        private uint _lastCommittedBatch;

        /// <summary>
        /// Proposes a new batch.
        /// </summary>
        public (uint reusePagesOlderThan, uint lastCommittedBatchId, IReadOnlyBatch read) Propose(IReadOnlyBatch read,
            ProposedBatch proposed)
        {
            // The ownership
            proposed.AcquireLease();

            lock (db._batchLock)
            {
                // Add by hash
                _proposedBatchesByHash.Add(proposed.StateHash, proposed);

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

                read.Dispose();

                if (_proposedBatchesByBatchId.Count > maxDepth)
                {
                    ScheduleFlush();
                }

                var next = db.BeginReadOnlyBatch();
                var minBatchId = db.CalculateMinBatchId(db.Root);

                return (minBatchId, _lastCommittedBatch, next);
            }
        }

        private void ScheduleFlush()
        {
        }

        public IHead Begin(in Keccak stateHash)
        {
            lock (db._batchLock)
            {
                var hash = Normalize(stateHash);

                var proposed = new List<ProposedBatch>();

                // The stateHash that is searched is proposed. We need to construct a list of dependencies.
                while (_proposedBatchesByHash.TryGetValue(hash, out var tail))
                {
                    proposed.Add(tail);
                    hash = tail.ParentHash;
                }

                // We want to have the oldest first
                proposed.Reverse();

                var read = (ReadOnlyBatch)db.BeginReadOnlyBatch(hash);

                var root = CreateNextRoot(read.Root, db._pool);
                var minBatchId = db.CalculateMinBatchId(root);

                return new HeadTrackingBatch(db, this, root, minBatchId, read, proposed.ToArray(), db._pool);
            }
        }

        private static Keccak Normalize(in Keccak keccak)
        {
            // pages are zeroed before, return zero on empty tree
            return keccak == Keccak.EmptyTreeHash ? Keccak.Zero : keccak;
        }

        public void Dispose()
        {
            foreach (var (_, proposed) in _proposedBatchesByHash)
            {
                proposed.Dispose();
            }
        }
    }

    // TODO: consider replacing the array with unmanaged version based on BufferPool, may allocate 2000 items per block which is ~16kb.
    private sealed class ProposedBatch((DbAddress at, Page page)[] changes, RootPage root, Keccak parentHash, BufferPool pool) : RefCountingDisposable
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
        private readonly MultiMultiHeadChain _chain;

        private readonly Dictionary<DbAddress, Page> _pageTable = new();
        private readonly Dictionary<Page, DbAddress> _pageTableReversed = new();
        private readonly List<(DbAddress at, Page page)> _cowed = new();

        // Linked list is used as it will have a FIFO behavior
        private readonly Queue<ProposedBatch> _proposed = new();

        // Current values, shifted with every commit
        private RootPage _root;
        private uint _batchId;
        private uint _reusePagesOlderThanBatchId;
        private IReadOnlyBatch _read;
        private Keccak _hash;

        public HeadTrackingBatch(PagedDb db, MultiMultiHeadChain chain, RootPage root,
            uint reusePagesOlderThanBatchId, IReadOnlyBatch read, IEnumerable<ProposedBatch> proposed,
            BufferPool pool) : base(db)
        {
            _chain = chain;
            _root = root;
            _batchId = root.Header.BatchId;
            _hash = root.Data.Metadata.StateHash;

            _pool = pool;
            _reusePagesOlderThanBatchId = reusePagesOlderThanBatchId;
            _read = read;

            foreach (var batch in proposed)
            {
                // As enqueued, acquire leases
                batch.AcquireLease();
                _proposed.Enqueue(batch);
            }
        }

        public void Commit(uint blockNumber, in Keccak blockHash)
        {
            SetMetadata(blockNumber, blockHash);

            // Copy the state hash
            _hash = Root.Data.Metadata.StateHash;

            // The root ownership is now moved to the proposed batch.
            // The batch is automatically leased by this head. It will be leased by the chain as well. 
            var batch = new ProposedBatch(_cowed.ToArray(), Root, _hash, Db._pool);

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
                    }
                }

                removed.Dispose();
            }

            // Amend local state so that it respects new
            _reusePagesOlderThanBatchId = reusePagesOlderThan;
            _root = CreateNextRoot(Root, _pool);
            _batchId = _root.Header.BatchId;
            _read = read;
        }

        public override Page GetAt(DbAddress address) => GetAtImpl(address, false);

        public override Page GetAtForWriting(DbAddress address) => GetAtImpl(address, true);

        public override void Prefetch(DbAddress addr)
        {
        }

        protected override void DisposeImpl()
        {
            var pool = Db._pool;

            pool.Return(_root.AsPage());
            _read.Dispose();

            // return all copies that were not proposed
            foreach (var (_, page) in _cowed)
            {
                pool.Return(page);
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

            // The entry did not exist before, create one
            var copy = CreateInMemoryOverride(addr, fromDb);
            _pageTable[addr] = copy;
            return copy;
        }

        private Page CreateInMemoryOverride(DbAddress at, Page source)
        {
            var page = _pool.Rent(false);

            // TODO: is this needed? Maybe not copy as it's for writes and will be overwritten?
            source.CopyTo(page);

            // Remember reversed mapping
            _pageTableReversed[page] = at;

            // Remember that it's proposed
            _cowed.Add((at, page));

            return page;
        }
    }
}

public interface IHead : IDataSetter, IDataGetter, IDisposable
{
    /// <summary>
    /// Commits the changes applied so far, and moves the head tracker to the next one.
    /// </summary>
    void Commit(uint blockNumber, in Keccak blockHash);
}

public interface IMultiHeadChain : IDisposable
{
    IHead Begin(in Keccak stateHash);
}