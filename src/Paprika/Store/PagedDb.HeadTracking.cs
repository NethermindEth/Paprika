using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Paprika.Chain;
using Paprika.Crypto;

namespace Paprika.Store;

public sealed partial class PagedDb
{
    private class HeadTracking(PagedDb db) : IHeadTracking
    {
        private readonly Dictionary<uint, List<ProposedBatch>> _proposedBatchesById = new();
        private readonly Dictionary<Keccak, ProposedBatch> _proposedBatchesByHash = new();

        /// <summary>
        /// Proposes a new batch.
        /// Additionally, it filters the <paramref name="alreadyProposed"/> selecting these that should be removed as they were applied to the database already.
        /// </summary>
        public (uint reusePagesOlderThan, IReadOnlyBatch read, ProposedBatch[] toRemove) Propose(IReadOnlyBatch read,
            ProposedBatch current, IEnumerable<ProposedBatch> alreadyProposed)
        {
            throw new NotImplementedException();
        }

        public IHead Begin(in Keccak stateHash)
        {
            lock (db._batchLock)
            {
                var hash = stateHash;

                var proposed = new List<ProposedBatch>();

                // The stateHash that is searched is proposed. We need to construct a list of dependencies.
                while (_proposedBatchesByHash.TryGetValue(hash, out var tail))
                {
                    proposed.Add(tail);
                    hash = proposed.ParentHash;
                }

                // We want to have the oldest first
                proposed.Reverse();

                var read = db.BeginReadOnlyBatch(hash);


                new HeadTrackingBatch(db, this, )


            }
        }
    }

    // TODO: consider replacing the array with unmanaged version based on BufferPool, may allocate 2000 items per block which is ~16kb.
    private record ProposedBatch((DbAddress at, Page page)[] Changes, RootPage Root, Keccak ParentHash);

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
        private readonly HeadTracking _tracking;

        private readonly Dictionary<DbAddress, Page> _pageTable = new();
        private readonly Dictionary<Page, DbAddress> _pageTableReversed = new();
        private readonly List<(DbAddress at, Page page)> _cowed = new();

        // Linked list is used as it will have a FIFO behavior
        private readonly LinkedList<ProposedBatch> _proposed = new();

        // Current values, shifted with every commit
        private RootPage _root;
        private uint _batchId;
        private uint _reusePagesOlderThanBatchId;
        private IReadOnlyBatch _read;

        public HeadTrackingBatch(PagedDb db, HeadTracking tracking, RootPage root,
            uint reusePagesOlderThanBatchId, IReadOnlyBatch read, IEnumerable<ProposedBatch> proposed, BufferPool pool) : base(db)
        {
            _tracking = tracking;
            _root = root;
            _batchId = root.Header.BatchId;

            _pool = pool;
            _reusePagesOlderThanBatchId = reusePagesOlderThanBatchId;
            _read = read;

            foreach (var batch in proposed)
            {
                _proposed.AddLast(batch);
            }
        }

        public void Commit()
        {
            // The root ownership is now moved to the proposed batch
            var batch = new ProposedBatch(_cowed.ToArray(), Root);
            _cowed.Clear();

            // Register proposal
            var (reusePagesOlderThan, read, toRemove) = _tracking.Propose(_read, batch, _proposed);

            // Locally track this proposed
            _proposed.AddLast(batch);

            // Remove pages to be removed
            // TODO: potentially make parallel, by gathering first keys to be removed and only then remove
            foreach (var removed in toRemove)
            {
                foreach (var (at, page) in removed.Changes)
                {
                    if (_pageTable.TryGetValue(at, out var actual) && page.Equals(actual))
                    {
                        _pageTable.Remove(at);
                        _pageTableReversed.Remove(actual);
                    }
                }

                _proposed.Remove(removed);
            }

            // Amend local state so that it respects new
            _reusePagesOlderThanBatchId = reusePagesOlderThan;
            _root = CreateNextRoot(Root, _pool);
            _batchId = _root.Header.BatchId;
            _read = read;
        }

        public override void Prefetch(DbAddress addr)
        {
        }

        protected override void DisposeImpl()
        {
        }

        public override uint BatchId => _batchId;

        protected override RootPage Root => _root;

        public ref readonly Metadata Metadata => ref _root.Data.Metadata;

        protected override uint ReusePagesOlderThanBatchId => _reusePagesOlderThanBatchId;

        public override DbAddress GetAddress(Page page) =>
            _pageTableReversed.TryGetValue(page, out var addr) ? addr : Db.GetAddress(page);

        protected override Page GetAtImpl(DbAddress addr, bool write)
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
                page = MakeCopy(addr, page);
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
            var copy = MakeCopy(addr, fromDb);
            _pageTable[addr] = page;
            return copy;
        }

        private Page MakeCopy(DbAddress at, Page source)
        {
            var page = _pool.Rent(false);
            source.CopyTo(page);

            // Remember reversed mapping
            _pageTableReversed[source] = at;

            // Remember that it's proposed
            _cowed.Add((at, page));

            return page;
        }
    }
}

public interface IHead : IDataSetter
{
    /// <summary>
    /// Commits the changes applied so far, and movest the head tracker to the next one.
    /// </summary>
    void Commit();

    /// <summary>
    /// The metadata of the last committed root.
    /// </summary>
    public ref readonly Metadata Metadata { get; }

    /// <summary>
    /// The batch id.
    /// </summary>
    public uint BatchId { get; }
}

public interface IHeadTracking
{
    IHead Begin(in Keccak stateHash);
}