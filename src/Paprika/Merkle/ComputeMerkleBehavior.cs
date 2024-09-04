using System.Buffers;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Diagnostics.Metrics;
using System.Runtime.InteropServices;
using Paprika.Chain;
using Paprika.Crypto;
using Paprika.Data;
using Paprika.RLP;
using Paprika.Store;
using Paprika.Utils;
using System.Diagnostics.CodeAnalysis;
using DataType = Paprika.Data.DataType;

namespace Paprika.Merkle;

/// <summary>
/// This is the Merkle component that works with the Merkle-agnostic part of Paprika.
///
/// It splits the Merkle part into two areas:
/// 1. building Merkle tree, that reads the data written in a given <see cref="ICommit"/> and applies it to create Merkle
///  construct.
/// 2. calls the computation of the Merkle RootHash when needed.
/// </summary>
/// <remarks>
/// Important: even though the underlying storage does COW, it's not done on the commit level. This means
/// that if there's an update for the given key, the key should be first read and worked with,
/// only to be updated at the end of the processing. Otherwise, the data using zero-copy could be overwritten.
/// </remarks>
public class ComputeMerkleBehavior : IPreCommitBehavior, IDisposable
{
    public const int ParallelismUnlimited = -1;
    public const int ParallelismNone = 1;

    internal const string MeterName = "Paprika.Merkle";

    internal const string HistogramStateProcessing = "State processing";
    internal const string HistogramStorageProcessing = "Storage processing";
    internal const string TotalMerkle = "Total Merkle";

    private readonly int _maxDegreeOfParallelism;

    // metrics
    private readonly Meter _meter;
    private readonly Histogram<long> _storageProcessing;
    private readonly Histogram<long> _stateProcessing;
    private readonly Histogram<long> _totalMerkle;
    private readonly BufferPool _pool;

    private static ParallelOptions s_parallelOptions = new()
    {
        // default to the number of processors
        MaxDegreeOfParallelism = Environment.ProcessorCount
    };

    /// <summary>
    /// Initializes the Merkle.
    /// </summary>
    /// <param name="maxDegreeOfParallelism">The maximum degree of parallelism aligned with <see cref="ParallelOptions.MaxDegreeOfParallelism"/>.</param>
    public ComputeMerkleBehavior(int maxDegreeOfParallelism = ParallelismUnlimited)
    {
        _maxDegreeOfParallelism = maxDegreeOfParallelism;
        s_parallelOptions = new()
        {
            // Override default with setting
            MaxDegreeOfParallelism = Math.Max(1, maxDegreeOfParallelism < 0 ? Environment.ProcessorCount : maxDegreeOfParallelism)
        };

        // Metrics
        _meter = new Meter(MeterName);

        _storageProcessing = _meter.CreateHistogram<long>(HistogramStateProcessing, "ms",
            "How long it takes to process state");
        _stateProcessing = _meter.CreateHistogram<long>(HistogramStorageProcessing, "ms",
            "How long it takes to process storage");
        _totalMerkle = _meter.CreateHistogram<long>(TotalMerkle, "ms",
            "How long it takes to process Merkle total");

        // Pool
        _pool = new BufferPool(128, true, _meter);
    }

    /// <summary>
    /// Calculates state root hash, passing through all the account and storage tries and building a new value
    /// that is not based on any earlier calculation. It's time consuming.
    /// </summary>
    public Keccak CalculateStateRootHash(IReadOnlyWorldState commit)
    {
        const ComputeHint hint = ComputeHint.ForceStorageRootHashRecalculation | ComputeHint.SkipCachedInformation;
        var wrapper = new CommitWrapper(commit, true);

        var root = Key.Merkle(NibblePath.Empty);

        UIntPtr stack = default;
        using var ctx = new ComputeContext(wrapper, TrieType.State, hint, CacheBudget.Options.None.Build(), _pool,
            ref stack);
        Compute(in root, ctx, out var value, out _);
        return value.Keccak;
    }

    public Keccak GetHash(NibblePath path, IReadOnlyWorldState commit, bool ignoreCache = false)
    {
        const ComputeHint hint = ComputeHint.None;
        var wrapper = new CommitWrapper(commit, true);

        UIntPtr stack = default;
        using var ctx = new ComputeContext(wrapper, TrieType.State, hint, CacheBudget.Options.None.Build(), _pool,
            ref stack);

        //TODO - this should be taken into account by CalculateHash method
        if (path.Length == NibblePath.KeccakNibbleCount)
        {
            //Merkle leaves are omitted at this height, so need to pick up keccak from parent (branch)
            var parentKey = Key.Merkle(path.SliceTo(path.Length - 1));
            using var merkleData = commit.Get(parentKey);
            if (!merkleData.Span.IsEmpty)
            {
                var leftover = Node.ReadFrom(out var parenType, out var leaf, out _, out var branch, merkleData.Span);
                if (parenType == Node.Type.Branch)
                {
                    if (!ignoreCache)
                    {
                        Span<byte> rlpMemoization = stackalloc byte[RlpMemo.Size];
                        RlpMemo memo = RlpMemo.Decompress(leftover, branch.Children, rlpMemoization);
                        if (memo.TryGetKeccak(path[NibblePath.KeccakNibbleCount - 1], out var keccakSpan))
                            return new Keccak(keccakSpan);
                    }
                    EncodeLeaf(Key.Merkle(path), ctx, NibblePath.Empty, out var keccakOrRlp, out var memoizeHint);
                    return keccakOrRlp.Keccak;
                }
                if (parenType == Node.Type.Leaf && leaf.Path[0] == path[path.Length - 1])
                {
                    EncodeLeaf(parentKey, ctx, leaf.Path, out var keccakOrRlp, out var memoizeHint);
                    return keccakOrRlp.Keccak;
                }
            }
        }

        var root = Key.Merkle(path);

        Compute(in root, ctx, out KeccakOrRlp hash, out _);
        return hash.Keccak;
    }

    public Keccak GetStorageHash(IReadOnlyWorldState commit, in Keccak account, NibblePath storagePath = default)
    {
        //TODO - this should be taken into account by CalculateHash method
        if (storagePath.Length == NibblePath.KeccakNibbleCount)
        {
            //Merkle leaves are omitted at this height, so need to pick up keccak from parent (branch)
            var branchKey = Key.Merkle(storagePath.SliceTo(storagePath.Length - 1));
            using var branchOwner = commit.Get(branchKey);
            if (!branchOwner.Span.IsEmpty)
            {
                var leftover = Node.ReadFrom(out var type, out var leaf, out var ext, out var branch, branchOwner.Span);
                if (type != Node.Type.Branch)
                    throw new Exception("Expected branch type");

                Span<byte> rlpMemoization = stackalloc byte[RlpMemo.Size];
                RlpMemo memo = RlpMemo.Decompress(leftover, branch.Children, rlpMemoization);
                if (memo.TryGetKeccak(storagePath[NibblePath.KeccakNibbleCount - 1], out var keccakSpan))
                    return new Keccak(keccakSpan);
            }
        }

        const ComputeHint hint = ComputeHint.DontUseParallel;
        var prefixed = new PrefixingCommit(new CommitWrapper(commit, true));
        prefixed.SetPrefix(account);

        var root = Key.Merkle(storagePath);
        UIntPtr stack = default;
        using var ctx = new ComputeContext(prefixed, TrieType.Storage, hint, CacheBudget.Options.None.Build(), _pool,
            ref stack);
        Compute(in root, ctx, out KeccakOrRlp hash, out _);
        return hash.Keccak;
    }

    class CommitWrapper : IChildCommit
    {
        private readonly IReadOnlyWorldState _readOnly;
        private readonly bool _allowChildCommits;

        public CommitWrapper(IReadOnlyWorldState readOnly, bool allowChildCommits = false)
        {
            _readOnly = readOnly;
            _allowChildCommits = allowChildCommits;
        }

        public ReadOnlySpanOwnerWithMetadata<byte> Get(scoped in Key key) => _readOnly.Get(key);

        public void Set(in Key key, in ReadOnlySpan<byte> payload, EntryType type)
        {
            // NOP
        }

        public void Set(in Key key, in ReadOnlySpan<byte> payload0, in ReadOnlySpan<byte> payload1, EntryType type)
        {
            // NOP
        }

        public IChildCommit GetChild() =>
            _allowChildCommits ? this : throw new NotImplementedException("Should not be called");

        public IReadOnlyDictionary<Keccak, int> Stats =>
            throw new NotImplementedException("Child commit provides no stats");

        void IDisposable.Dispose()
        {
        }

        void IChildCommit.Commit()
        {
        }
    }

    public Keccak BeforeCommit(ICommit commit, CacheBudget budget)
    {
        using var total = _totalMerkle.Measure();

        // There are following things to do:
        // 1. Visit Storage Tries
        // 2. Calculate Storage Roots
        // 3. Visit State Root
        // 4. Calculate State Root
        //
        // 1. and 2. do the same for the tries. They make the structure right (delete empty accounts, create/delete nodes etc)
        // and they invalidate memoized keccaks.
        // 2. is a prerequisite for 3.
        // 1. and 3 are prerequisites for 4.
        //
        // This means that 1 can be run in parallel with 2.
        // 3. Can be run in parallel for each tree
        // 4. Can be parallelized at the root level

        using (_storageProcessing.Measure())
        {
            if (_maxDegreeOfParallelism == ParallelismNone)
            {
                ProcessStorageSingleThreaded(commit, budget);
            }
            else
            {
                ScatterGather(commit, GetStorageWorkItems(commit, budget));
            }
        }

        using (_stateProcessing.Measure())
        {
            new BuildStateTreeItem(commit, commit.Stats.Keys, budget, _pool).DoWork();

            var root = Key.Merkle(NibblePath.Empty);
            UIntPtr stack = default;
            var hint = _maxDegreeOfParallelism == ParallelismNone ? ComputeHint.DontUseParallel : ComputeHint.None;
            using var ctx = new ComputeContext(commit, TrieType.State, hint, budget, _pool, ref stack);
            Compute(root, ctx, out var rootKeccak, out _);

            Debug.Assert(rootKeccak.DataType == KeccakOrRlp.Type.Keccak);

            RootHash = rootKeccak.Keccak;

            return RootHash;
        }
    }

    public void RecalculateStorageTries(ICommit commit, CacheBudget budget)
    {
        using (_storageProcessing.Measure())
        {
            if (_maxDegreeOfParallelism == ParallelismNone)
            {
                ProcessStorageSingleThreaded(commit, budget);
            }
            else
            {
                ScatterGather(commit, GetStorageWorkItems(commit, budget));
            }
        }
    }

    private void ProcessStorageSingleThreaded(ICommit commit, CacheBudget budget)
    {
        var prefixed = new PrefixingCommit(commit);

        var page = _pool.Rent(false);

        // Visit changes and build trees
        try
        {
            commit.Visit((in Key key, ReadOnlySpan<byte> value) =>
            {
                var keccak = key.Path.UnsafeAsKeccak;
                prefixed.SetPrefix(keccak);

                if (value.IsEmpty)
                {
                    Delete(in key.StoragePath, 0, prefixed, budget);
                }
                else
                {
                    MarkPathDirty(in key.StoragePath, page.Span, prefixed, budget, TrieType.Storage);
                }
            }, TrieType.Storage);
        }
        finally
        {
            _pool.Return(page);
        }

        // Calculate and update accounts
        foreach (var (keccak, value) in commit.Stats)
        {
            var hasSStores = value > 0;
            if (hasSStores)
            {
                prefixed.SetPrefix(keccak);
                BuildStorageTriesItem.CalculateStorageRoot(keccak, this, budget, prefixed, commit);
            }
        }
    }

    /// <summary>
    /// Runs the work items in parallel then gathers the data and commits to the parent.
    /// </summary>
    /// <param name="commit">The original commit.</param>
    /// <param name="workItems">The work items.</param>
    private static void ScatterGather(ICommit commit, BuildStorageTriesItem[] workItems)
    {
        if (workItems.Length == 0)
        {
            return;
        }

        if (workItems.Length == 1)
        {
            // Direct work on commit as there are no other interfering work items; 
            workItems[0].DoWork(commit);
            return;
        }

        var children = new ConcurrentQueue<IChildCommit>();
        Parallel.For(0, workItems.Length, s_parallelOptions,
            commit.GetChild,
            (i, _, child) =>
            {
                workItems[i].DoWork(child);
                return child;
            },
            children.Enqueue
        );

        while (children.TryDequeue(out var child))
        {
            child.Commit();
            child.Dispose();
        }
    }

    public void OnNewAccountCreated(in Keccak address, ICommit commit)
    {
        // Set a transient empty entry for the newly created account.
        // This simulates an empty storage tree.
        // If this account has storage set, it won't try to query the database to get nothing, it will get nothing from here.
        commit.Set(Key.Merkle(NibblePath.FromKey(address)), ReadOnlySpan<byte>.Empty, EntryType.UseOnce);
    }

    public void OnAccountDestroyed(in Keccak address, ICommit commit)
    {
        // Set an empty entry as the storage root for the destroyed account.
        // This simulates an empty storage tree. 
        // If this account has storage set, it won't try to query the database to get nothing, it will get nothing from here.
        commit.Set(Key.Merkle(NibblePath.FromKey(address)), ReadOnlySpan<byte>.Empty, EntryType.UseOnce);
    }

    /// <summary>
    /// Builds works items responsible for building up the storage tries.
    /// </summary>
    private BuildStorageTriesItem[] GetStorageWorkItems(ICommit commit, CacheBudget budget)
    {
        return commit.Stats
            .Where(kvp => kvp.Value > 0)
            .Select(kvp => new BuildStorageTriesItem(this, commit, kvp.Key, budget, _pool))
            .ToArray();
    }

    public const int SkipRlpMemoizationForTopLevelsCount = 2;

    public ReadOnlySpan<byte> InspectBeforeApply(in Key key, ReadOnlySpan<byte> data, Span<byte> workingSet)
    {
        if (data.IsEmpty)
            return data;

        if (key.Type != DataType.Merkle)
            return data;

        var node = Node.Header.Peek(data).NodeType;

        if (node != Node.Type.Branch)
        {
            // Return data as is, either the node is not a branch or the memoization is not set for branches.
            return data;
        }

        if (key.Path.Length < SkipRlpMemoizationForTopLevelsCount)
        {
            // For State branches, omit top levels of RLP memoization
            return Node.Branch.GetOnlyBranchData(data);
        }

        var memoizedRlp = Node.Branch.ReadFrom(data, out var branch);
        if (memoizedRlp.Length == 0)
        {
            // no RLP of children memoized, return
            return data;
        }

        Debug.Assert(memoizedRlp.Length == RlpMemo.Size);

        // There are RLPs here, compress them
        var dataLength = data.Length - RlpMemo.Size;
        data[..dataLength].CopyTo(workingSet);

        var compressedLength = RlpMemo.Compress(key, memoizedRlp, branch.Children, workingSet[dataLength..]);

        return workingSet[..(dataLength + compressedLength)];
    }

    public Keccak RootHash { get; private set; }

    [Flags]
    private enum ComputeHint
    {
        None = 0,

        /// <summary>
        /// Skip cached Branch keccaks as well as memoized RLP.
        /// </summary>
        SkipCachedInformation = 1,

        /// <summary>
        /// Don't user parallel computation when calculating this computation.
        /// </summary>
        DontUseParallel = 2,

        /// <summary>
        /// Forces the recalculation of the storage root hash.
        /// </summary>
        ForceStorageRootHashRecalculation = 4
    }

    private readonly ref struct ComputeContext
    {
        public readonly ICommit Commit;
        public readonly TrieType TrieType;
        public readonly ComputeHint Hint;
        public readonly CacheBudget Budget;

        private readonly BufferPool _pool;
        private readonly ref UIntPtr _root;

        public ComputeContext(ICommit commit, TrieType trieType, ComputeHint hint, CacheBudget budget, BufferPool pool,
            ref UIntPtr root)
        {
            Commit = commit;
            TrieType = trieType;
            Hint = hint;
            Budget = budget;

            _pool = pool;
            _root = ref root;
        }

        public PageOwner Rent() => PageOwner.Rent(_pool, ref _root);

        public void Dispose() => PageOwner.ReturnStack(_pool, ref _root);
    }

    private void Compute(scoped in Key key, scoped in ComputeContext ctx, out KeccakOrRlp keccakOrRlp, out bool memoizeHint)
    {
        using var owner = ctx.Commit.Get(key);
        memoizeHint = true;

        // The computation might be done for a node that was not traversed and might require a cache
        if (ctx.Budget.ShouldCache(owner, out var entryType))
        {
            ctx.Commit.Set(key, owner.Span, entryType);
        }

        if (owner.IsEmpty)
        {
            // empty tree, return empty
            keccakOrRlp = Keccak.EmptyTreeHash;
            return;
        }

        var leftover = Node.ReadFrom(out var type, out var leaf, out var ext, out var branch, owner.Span);
        switch (type)
        {
            case Node.Type.Leaf:
                EncodeLeaf(key, ctx, leaf.Path, out keccakOrRlp, out memoizeHint);
                return;
            case Node.Type.Extension:
                EncodeExtension(key, ctx, ext, out keccakOrRlp);
                return;
            case Node.Type.Branch:
                EncodeBranch(key, ctx, branch, leftover, owner.IsOwnedBy(ctx.Commit), out keccakOrRlp);
                return;
            default:
                ThrowOutOfRange();
                keccakOrRlp = default;
                return;
        }

        [DoesNotReturn]
        [StackTraceHidden]
        static void ThrowOutOfRange()
        {
            throw new ArgumentOutOfRangeException();
        }
    }

    [SkipLocalsInit]
    private void EncodeLeaf(scoped in Key key, scoped in ComputeContext ctx, scoped in NibblePath leafPath, out KeccakOrRlp keccakOrRlp, out bool memoizeHint)
    {
        var leafTotalPath =
            key.Path.Append(leafPath, stackalloc byte[NibblePath.MaxLengthValue * 2 + 1]);

        var leafKey = ctx.TrieType == TrieType.State
            ? Key.Account(leafTotalPath)
            // the prefix will be added by the prefixing commit
            : Key.Raw(leafTotalPath, DataType.StorageCell, NibblePath.Empty);

        using var leafData = ctx.Commit.Get(leafKey);
        EncodeLeafByPath(leafKey, ctx, leafPath, leafData, out keccakOrRlp, out memoizeHint);
    }

    [SkipLocalsInit]
    private void EncodeLeafByPath(
        scoped in Key leafKey,
        scoped in ComputeContext ctx,
        scoped in NibblePath leafPath,
        scoped in ReadOnlySpanOwnerWithMetadata<byte> leafData,
        out KeccakOrRlp keccakOrRlp,
        out bool memoizeHint)
    {
        memoizeHint = true;
        keccakOrRlp = Keccak.EmptyTreeHash;
#if SNAP_SYNC_SUPPORT
        NibblePath pathEncodedKeccak = leafKey.Path;
        if (leafData.IsEmpty && leafKey.Path.Length == NibblePath.KeccakNibbleCount)
        {
            using var merkleData = ctx.Commit.Get(Key.Merkle(leafKey.Path));
            if (!merkleData.IsEmpty)
            {
                var leftover = Node.ReadFrom(out var type, out var leaf, out var ext, out var branch, merkleData.Span);
                if (type == Node.Type.Leaf)
                    pathEncodedKeccak = leaf.Path;
            }
        }
        if (SnapSync.TryGetKeccakFromBoundaryPath(pathEncodedKeccak, out var keccak))
        {
            memoizeHint = false;
            keccakOrRlp = keccak;
            return;
        }
#endif

        if (leafData.IsEmpty)
            return;

        // leaf data might be coming from the db, potentially cache them
        if (ctx.Budget.ShouldCache(leafData, out var entryType))
        {
            ctx.Commit.Set(leafKey, leafData.Span, entryType);
        }

        if (ctx.TrieType == TrieType.State)
        {
            Account.ReadFrom(leafData.Span, out var account);

            if (ctx.Hint.HasFlag(ComputeHint.ForceStorageRootHashRecalculation))
            {
                var prefixed = new PrefixingCommit(ctx.Commit);
                prefixed.SetPrefix(leafKey.Path);
                UIntPtr stack = default;
                using var ctx2 = new ComputeContext(prefixed, TrieType.Storage, ctx.Hint, ctx.Budget, _pool, ref stack);
                Compute(Key.Merkle(NibblePath.Empty), ctx2, out keccakOrRlp, out _);
                account = new Account(account.Balance, account.Nonce, account.CodeHash, keccakOrRlp.Keccak);
            }

            Node.Leaf.KeccakOrRlp(leafPath, account, out keccakOrRlp);
            return;
        }

        Debug.Assert(ctx.TrieType == TrieType.Storage, "Only storage now");

        Node.Leaf.KeccakOrRlp(leafPath, leafData.Span, out keccakOrRlp);
    }

    private void EncodeBranch(scoped in Key key, scoped in ComputeContext ctx, scoped in Node.Branch branch,
        ReadOnlySpan<byte> previousRlp, bool isOwnedByThisCommit, out KeccakOrRlp keccakOrRlp)
    {
        // Parallelize at the root level any trie, state or storage, that have all children set.
        // This heuristic is used to estimate that the tree should be big enough to gain from making this computation
        // parallel but without calculating and storing additional information how big is the tree.
        var runInParallel = !ctx.Hint.HasFlag(ComputeHint.DontUseParallel) && key.Path.IsEmpty &&
                            branch.Children.AllSet;

        var memoize = !ctx.Hint.HasFlag(ComputeHint.SkipCachedInformation);

        using var buffer = ctx.Rent();

        // divide buffer
        const int rlpSlice = 1024;

        var rlp = buffer.Span[..rlpSlice];
        var rlpMemoization = buffer.Span.Slice(rlpSlice, RlpMemo.Size);
        var memoizedUpdated = false;

        RlpMemo memo = default;

        if (memoize)
        {
            var childRlpRequiresUpdate = isOwnedByThisCommit == false || previousRlp.Length != RlpMemo.Size;
            memo = childRlpRequiresUpdate
                ? RlpMemo.Decompress(previousRlp, branch.Children, rlpMemoization)
                : new RlpMemo(MakeRlpWritable(previousRlp));
        }

        // leave for length preamble
        const int initialShift = Rlp.MaxLengthOfLength + 1;

        var stream = new RlpStream(rlp)
        {
            Position = initialShift
        };

        if (!runInParallel)
        {
            var childSpan = buffer.Span[(RlpMemo.Size + rlpSlice)..];

            for (byte i = 0; i < NibbleSet.NibbleCount; i++)
            {
                if (branch.Children[i])
                {
                    if (memoize && memo.TryGetKeccak(i, out var keccak))
                    {
                        // keccak from cache
                        stream.Encode(keccak);
                        continue;
                    }

                    var childPath = key.Path.AppendNibble(i, childSpan);
                    var leafKey = Key.Merkle(childPath);

                    bool memoizeHint = true;
                    if (childPath.Length == NibblePath.KeccakNibbleCount)
                    {
                        NibblePath leafKeyPath = NibblePath.Empty;
#if SNAP_SYNC_SUPPORT
                        using var owner = ctx.Commit.Get(leafKey);
                        if (!owner.IsEmpty)
                        {
                            Node.ReadFrom(out var type, out var leaf, out var ext, out var br, owner.Span);
                            switch (type)
                            {
                                case Node.Type.Leaf:
                                    leafKeyPath = leaf.Path;
                                    break;
                                default:
                                    throw new InvalidOperationException("Unexpected node type when encoding leaf");
                            }
                        }
#endif
                        EncodeLeaf(leafKey, ctx, leafKeyPath, out keccakOrRlp, out memoizeHint);
                    }
                    else
                    {
                        Compute(leafKey, ctx, out keccakOrRlp, out memoizeHint);
                    }

                    // it's either Keccak or a span. Both are encoded the same ways
                    if (keccakOrRlp.DataType == KeccakOrRlp.Type.Keccak)
                    {
                        stream.Encode(keccakOrRlp.Keccak);
                    }
                    else
                    {
                        stream.Write(keccakOrRlp.Span);
                    }

                    if (memoize && memoizeHint)
                    {
                        memoizedUpdated = true;
                        memo.Set(keccakOrRlp, i);
                    }
                }
                else
                {
                    stream.EncodeEmptyArray();

                    if (memoize)
                    {
                        memo.Clear(i);
                        memoizedUpdated = true;
                    }
                }
            }
        }
        else
        {
            // materialize path so that it can be closure captured
            var results = new byte[NibbleSet.NibbleCount][];
            var commits = new IChildCommit[NibbleSet.NibbleCount];

            var commit = ctx.Commit;
            var trieType = ctx.TrieType;
            var hint = ctx.Hint;
            var budget = ctx.Budget;

            // parallel calculation
            bool[] memoizeHint = new bool[NibbleSet.NibbleCount];
            Parallel.For((long)0, NibbleSet.NibbleCount, s_parallelOptions, nibble =>
            {
                var childPath = NibblePath.Single((byte)nibble, 0);

                var child = commits[nibble] = commit.GetChild();
                UIntPtr stack = default;
                using var ctx = new ComputeContext(child, trieType, hint, budget, _pool, ref stack);
                Compute(Key.Merkle(childPath), ctx, out KeccakOrRlp keccakRlp, out memoizeHint[nibble]);
                results[nibble] = keccakRlp.Span.ToArray();
            });

            foreach (var childCommit in commits)
            {
                childCommit.Commit();
                childCommit.Dispose();
            }

            // write all results in the stream
            for (byte i = 0; i < NibbleSet.NibbleCount; i++)
            {
                // it's either Keccak or a span. Both are encoded the same ways
                var value = results[i];

                stream.Encode(value);
                if (memoize && memoizeHint[i])
                {
                    if (value.Length == Keccak.Size)
                    {
                        memoizedUpdated = true;
                        memo.SetRaw(value, i);
                    }
                    else
                    {
                        memoizedUpdated = true;
                        memo.Clear(i);
                    }
                }
            }
        }

        // no value at branch, write empty array at the end
        stream.EncodeEmptyArray();

        // Write length of length in front of the payload, resetting the stream properly
        var end = stream.Position;
        var actualLength = end - initialShift;
        var lengthOfLength = Rlp.LengthOfLength(actualLength) + 1;
        var from = initialShift - lengthOfLength;
        stream.Position = from;
        stream.StartSequence(actualLength);

        if (memoize && !isOwnedByThisCommit && memoizedUpdated)
        {
            //
            ctx.Commit.SetBranch(key, branch.Children, rlpMemoization, EntryType.Persistent);
        }

        KeccakOrRlp.FromSpan(rlp.Slice(from, end - from), out keccakOrRlp);
    }

    /// <summary>
    /// Makes the RLP writable. Should be used only after ensuring that the current <see cref="ICommit"/>
    /// is the owner of the span. This can be done by using <see cref="ReadOnlySpanOwner{T}.IsOwnedBy"/>.
    /// </summary>
    public static Span<byte> MakeRlpWritable(ReadOnlySpan<byte> previousRlp) =>
        MemoryMarshal.CreateSpan(ref MemoryMarshal.GetReference(previousRlp), previousRlp.Length);

    private void EncodeExtension(scoped in Key key, scoped in ComputeContext ctx, scoped in Node.Extension ext, out KeccakOrRlp keccakOrRlp)
    {
        using var pooled = ctx.Rent();

        const int slice = 1024;
        var span = pooled.Span[..slice];

        // retrieve the children keccak-or-rlp
        Compute(Key.Merkle(key.Path.Append(ext.Path, span)), ctx, out keccakOrRlp, out _);

        ext.Path.HexEncode(span, false);
        span = span.Slice(0, ext.Path.HexEncodedLength); // trim the span to the hex

        var contentLength = Rlp.LengthOf(span) + (keccakOrRlp.DataType == KeccakOrRlp.Type.Rlp
            ? keccakOrRlp.Length
            : Rlp.LengthOfKeccakRlp);

        var totalLength = Rlp.LengthOfSequence(contentLength);

        RlpStream stream = new(pooled.Span.Slice(slice, totalLength));
        stream.StartSequence(contentLength);
        stream.Encode(span);
        stream.Encode(keccakOrRlp.Keccak);
        stream.ToKeccakOrRlp(out keccakOrRlp);
    }

    /// <summary>
    /// This component appends the prefix to all the commit operations.
    /// It's useful for storage operations, that have their key prefixed with the account.
    /// </summary>
    public class PrefixingCommit : ICommit
    {
        private readonly ICommit _commit;
        private Keccak _keccak;

        public PrefixingCommit(ICommit commit)
        {
            _commit = commit;
        }

        public void SetPrefix(in NibblePath path) => SetPrefix(path.UnsafeAsKeccak);

        public void SetPrefix(in Keccak keccak) => _keccak = keccak;

        public ReadOnlySpanOwnerWithMetadata<byte> Get(scoped in Key key) =>
            _commit.Get(Build(key));

        public void Set(in Key key, in ReadOnlySpan<byte> payload, EntryType type) =>
            _commit.Set(Build(key), in payload, type);

        public void Set(in Key key, in ReadOnlySpan<byte> payload0, in ReadOnlySpan<byte> payload1, EntryType type)
            => _commit.Set(Build(key), payload0, payload1, type);

        /// <summary>
        /// Builds the <see cref="_keccak"/> aware key, treating the path as the path for the storage.
        /// </summary>
        private Key Build(scoped in Key key) => Key.Raw(NibblePath.FromKey(_keccak), key.Type, key.Path);

        public IChildCommit GetChild() => new ChildCommit(this, _commit.GetChild());

        public void Visit(CommitAction action, TrieType type) => throw new Exception("Should not be called");

        public IReadOnlyDictionary<Keccak, int> Stats =>
            throw new NotImplementedException("No stats for the child commit");

        private class ChildCommit : IChildCommit
        {
            private readonly PrefixingCommit _parent;
            private readonly IChildCommit _commit;

            public ChildCommit(PrefixingCommit parent, IChildCommit commit)
            {
                _parent = parent;
                _commit = commit;
            }

            public ReadOnlySpanOwnerWithMetadata<byte> Get(scoped in Key key) =>
                _commit.Get(_parent.Build(key));

            public void Set(in Key key, in ReadOnlySpan<byte> payload, EntryType type) =>
                _commit.Set(_parent.Build(key), payload, type);

            public void Set(in Key key, in ReadOnlySpan<byte> payload0, in ReadOnlySpan<byte> payload1,
                EntryType type) =>
                _commit.Set(_parent.Build(key), payload0, payload1, type);

            public void Dispose() => _commit.Dispose();

            public void Commit() => _commit.Commit();

            public IChildCommit GetChild() => new ChildCommit(_parent, _commit.GetChild());

            public IReadOnlyDictionary<Keccak, int> Stats =>
                throw new NotImplementedException("No stats for the child commit");
        }
    }

    private enum DeleteStatus
    {
        KeyDoesNotExist,

        /// <summary>
        /// Happens when a leaf is deleted.
        /// </summary>
        LeafDeleted,

        /// <summary>
        /// Happens when a branch turns into a leaf or extension.
        /// </summary>
        BranchToLeafOrExtension,

        /// <summary>
        /// Happens when an extension turns into a leaf.
        /// </summary>
        ExtensionToLeaf,

        NodeTypePreserved
    }

    /// <summary>
    /// Deletes the given path, providing information whether the node has changed its type.
    /// </summary>
    /// <returns>Whether the node has changed its type </returns>
    /// <exception cref="ArgumentOutOfRangeException"></exception>
    [SkipLocalsInit]
    private static DeleteStatus Delete(in NibblePath path, int at, ICommit commit, CacheBudget budget)
    {
        var slice = path.SliceTo(at);
        var key = Key.Merkle(slice);

        var leftoverPath = path.SliceFrom(at);

        using var owner = commit.Get(key);
        if (owner.IsEmpty)
        {
            return DeleteStatus.KeyDoesNotExist;
        }

        // read the existing one
        var leftover = Node.ReadFrom(out var type, out var leaf, out var ext, out var branch, owner.Span);
        switch (type)
        {
            case Node.Type.Leaf:
                {
                    var diffAt = leaf.Path.FindFirstDifferentNibble(leftoverPath);

                    if (diffAt == leaf.Path.Length)
                    {
                        commit.DeleteKey(key);
                        return DeleteStatus.LeafDeleted;
                    }

                    return DeleteStatus.KeyDoesNotExist;
                }
            case Node.Type.Extension:
                {
                    var diffAt = ext.Path.FindFirstDifferentNibble(leftoverPath);
                    if (diffAt != ext.Path.Length)
                    {
                        // the path does not follow the extension path. It does not exist
                        return DeleteStatus.KeyDoesNotExist;
                    }

                    var newAt = at + ext.Path.Length;
                    var status = Delete(path, newAt, commit, budget);

                    if (status == DeleteStatus.KeyDoesNotExist)
                    {
                        // the child reported not existence
                        return DeleteStatus.KeyDoesNotExist;
                    }

                    if (status == DeleteStatus.NodeTypePreserved)
                    {
                        if (budget.ShouldCache(owner, out var entryType))
                        {
                            commit.SetExtension(key, ext.Path, entryType);
                        }

                        // The node has not changed its type
                        return DeleteStatus.NodeTypePreserved;
                    }

                    Debug.Assert(status == DeleteStatus.BranchToLeafOrExtension, $"Unexpected status of {status}");

                    var childPath = path.SliceTo(newAt);
                    var childKey = Key.Merkle(childPath);

                    return TransformExtension(childKey, commit, key, ext);
                }
            case Node.Type.Branch:
                {
                    var nibble = path[at];
                    if (!branch.Children[nibble])
                    {
                        // no such child
                        return DeleteStatus.KeyDoesNotExist;
                    }

                    var newAt = at + 1;

                    var status = LeafCanBeOmitted(newAt) ? DeleteStatus.LeafDeleted : Delete(path, newAt, commit, budget);
                    if (status == DeleteStatus.KeyDoesNotExist)
                    {
                        // child reports non-existence
                        return DeleteStatus.KeyDoesNotExist;
                    }

                    if (status
                        is DeleteStatus.NodeTypePreserved
                        or DeleteStatus.ExtensionToLeaf
                        or DeleteStatus.BranchToLeafOrExtension)
                    {
                        UpdateBranchOnDelete(commit, branch, branch.Children, leftover, owner, nibble, key);
                        return DeleteStatus.NodeTypePreserved;
                    }

                    Debug.Assert(status == DeleteStatus.LeafDeleted, "leaf deleted");

                    var updatedChildren = branch.Children.Remove(nibble);

                    // if branch has still more than one child, just update the set
                    if (updatedChildren.SetCount > 1)
                    {
                        UpdateBranchOnDelete(commit, branch, updatedChildren, leftover, owner, nibble, key);
                        return DeleteStatus.NodeTypePreserved;
                    }

                    // there's an only child now. The branch should be collapsed
                    var onlyNibble = updatedChildren.SmallestNibbleSet;
                    var onlyChildPath = slice.AppendNibble(onlyNibble,
                        stackalloc byte[slice.MaxByteLength + 1]);

                    var onlyChildKey = Key.Merkle(onlyChildPath);
                    using var onlyChildSpanOwner = commit.Get(onlyChildKey);

                    // need to collapse the branch
                    var childType = Node.ReadFrom(out var childLeaf, out var childExt, onlyChildSpanOwner.Span);

                    var firstNibblePath = NibblePath.Single(onlyNibble, odd: 1 - newAt % 2);

                    if (childType == Node.Type.Extension)
                    {
                        var extensionPath = firstNibblePath.Append(childExt.Path,
                            stackalloc byte[NibblePath.FullKeccakByteLength]);

                        // delete the only child
                        commit.DeleteKey(onlyChildKey);

                        // the single child is an extension, make it an extension
                        commit.SetExtension(key, extensionPath);

                        return DeleteStatus.BranchToLeafOrExtension;
                    }

                    if (childType == Node.Type.Branch)
                    {
                        // the single child is an extension, make it an extension with length of 1
                        commit.SetExtension(key, firstNibblePath);
                        return DeleteStatus.BranchToLeafOrExtension;
                    }

                    // prepare the new leaf path
                    var leafPath =
                        firstNibblePath.Append(childLeaf.Path, stackalloc byte[NibblePath.FullKeccakByteLength]);

                    // replace branch with the leaf
                    commit.SetLeaf(key, leafPath);

                    // delete the only child
                    commit.DeleteKey(onlyChildKey);

                    return DeleteStatus.BranchToLeafOrExtension;
                }
            default:
                return ThrowUnknownType();
        }

        static void UpdateBranchOnDelete(ICommit commit, in Node.Branch branch, NibbleSet.Readonly children,
            ReadOnlySpan<byte> leftover, ReadOnlySpanOwnerWithMetadata<byte> owner, byte nibble, in Key key)
        {
            var childRlpRequiresUpdate = owner.IsOwnedBy(commit) == false || leftover.Length != RlpMemo.Size;
            RlpMemo memo;
            byte[]? rlpWorkingSet = null;

            if (childRlpRequiresUpdate)
            {
                // TODO: make it a context and pass through all the layers
                rlpWorkingSet = ArrayPool<byte>.Shared.Rent(RlpMemo.Size);
                memo = RlpMemo.Decompress(leftover, branch.Children, rlpWorkingSet.AsSpan());
            }
            else
            {
                memo = new RlpMemo(MakeRlpWritable(leftover));
            }

            memo.Clear(nibble);

            var shouldUpdate = !branch.Children.Equals(children);

            // There's the cached RLP
            if (shouldUpdate || childRlpRequiresUpdate)
            {
                commit.SetBranch(key, children, memo.Raw);
            }

            if (rlpWorkingSet != null)
            {
                ArrayPool<byte>.Shared.Return(rlpWorkingSet);
            }
        }

        static DeleteStatus ThrowUnknownType()
        {
            throw new ArgumentOutOfRangeException();
        }
    }

    private static bool LeafCanBeOmitted(int pathLength)
    {
        return pathLength == NibblePath.KeccakNibbleCount;
    }

    /// <summary>
    /// Transforms the extension either to a <see cref="Node.Type.Leaf"/> or to a longer <see cref="Node.Type.Extension"/>.
    /// </summary>
    [SkipLocalsInit]
    private static DeleteStatus TransformExtension(in Key childKey, ICommit commit, in Key key, in Node.Extension ext)
    {
        using var childOwner = commit.Get(childKey);

        // TODO: this should be not needed but for some reason the ownership of the owner breaks memory safety here
        Span<byte> copy = stackalloc byte[childOwner.Span.Length];
        childOwner.Span.CopyTo(copy);

        var childType = Node.ReadFrom(out var childLeaf, out var childExt, copy);
        if (childType == Node.Type.Extension)
        {
            // it's E->E, merge extensions into a single extension with concatenated path
            commit.DeleteKey(childKey);
            commit.SetExtension(key,
                ext.Path.Append(childExt.Path, stackalloc byte[NibblePath.FullKeccakByteLength]));

            return DeleteStatus.NodeTypePreserved;
        }

        // it's E->L, merge them into a leaf
        commit.DeleteKey(childKey);
        commit.SetLeaf(key,
            ext.Path.Append(childLeaf.Path, stackalloc byte[NibblePath.FullKeccakByteLength]));

        return DeleteStatus.ExtensionToLeaf;
    }

    [SkipLocalsInit]
    private static void MarkPathDirty(in NibblePath path, in Span<byte> rlpMemoWorkingSet, ICommit commit,
        CacheBudget budget, TrieType trieType)
    {
        // Flag forcing the leaf creation, that saves one get of the non-existent value.
        var createLeaf = false;

        Span<byte> span = stackalloc byte[33];

        for (var i = 0; i <= path.Length; i++)
        {
            var slice = path.SliceTo(i);
            var key = Key.Merkle(slice);
            var leftoverPath = path.SliceFrom(i);

            // The creation of the leaf is forced, create and return.
            if (createLeaf)
            {
                SetLeaf(commit, key, leftoverPath, trieType);
                return;
            }

            // Query for the node
            using var owner = commit.Get(key);
            if (owner.IsEmpty)
            {
                // No value set now, create one.
                SetLeaf(commit, key, leftoverPath, trieType);
                return;
            }

            // read the existing one
            var leftover = Node.ReadFrom(out var type, out var leaf, out var ext, out var branch, owner.Span);
            switch (type)
            {
                case Node.Type.Leaf:
                    {
#if SNAP_SYNC_SUPPORT
                        if (SnapSync.IsBoundaryHashLeaf(leaf))
                        {
                            var keyType = trieType == TrieType.State ? DataType.Account : DataType.StorageCell;
                            var valueKey = Key.Raw(slice, keyType, NibblePath.Empty);

                            // delete memoized keccak
                            //commit.Set(valueKey, ReadOnlySpan<byte>.Empty);

                            var newDataKey = Key.Raw(path, keyType, NibblePath.Empty);
                            var newData = commit.Get(newDataKey);

                            if (!SnapSync.IsBoundaryValue(newData.Span))
                            {
                                // commit the new leaf
                                commit.SetLeaf(key, leftoverPath);
                                return;
                            }

                            return;
                        }
#endif

                        var diffAt = leaf.Path.FindFirstDifferentNibble(leftoverPath);

                        if (diffAt == leaf.Path.Length)
                        {
                            if (budget.ShouldCache(owner, out var cacheType))
                            {
                                commit.SetLeaf(key, leftoverPath, cacheType);
                            }

                            return;
                        }

                        var nibbleA = leaf.Path[diffAt];
                        var nibbleB = leftoverPath[diffAt];

                        // nibbleA, deep copy to write in an unsafe manner
                        var pathA = path.SliceTo(i + diffAt).AppendNibble(nibbleA, span);
                        SetLeaf(commit, Key.Merkle(pathA), leaf.Path.SliceFrom(diffAt + 1), trieType);

                        // nibbleB, set the newly set leaf, slice to the next nibble
                        var pathB = path.SliceTo(i + 1 + diffAt);
                        SetLeaf(commit, Key.Merkle(pathB), leftoverPath.SliceFrom(diffAt + 1), trieType);

                        // Important! Make it the last in set of changes as it may be updating the key that was read (leaf)
                        if (diffAt > 0)
                        {
                            // diff is not on the 0th position, so it will be a branch but preceded with an extension
                            commit.SetExtension(key, leftoverPath.SliceTo(diffAt));
                        }

                        // Important! Make it the last in set of changes
                        var branchKey = Key.Merkle(path.SliceTo(i + diffAt));
                        commit.SetBranch(branchKey, new NibbleSet(nibbleA, nibbleB));

                        return;
                    }
                case Node.Type.Extension:
                    {
                        var diffAt = ext.Path.FindFirstDifferentNibble(leftoverPath);
                        if (diffAt == ext.Path.Length)
                        {
                            // the path overlaps with what is there, move forward
                            i += ext.Path.Length - 1;

                            if (budget.ShouldCache(owner, out var entryType))
                            {
                                commit.SetExtension(key, ext.Path, entryType);
                            }

                            continue;
                        }

                        if (diffAt == 0)
                        {
                            if (ext.Path.Length == 1)
                            {
                                // special case of an extension being only 1 nibble long
                                // 1. replace an extension with a branch
                                // 2. leave the next branch as is
                                // 3. add a new leaf
                                var set = new NibbleSet(ext.Path[0], leftoverPath[0]);
                                commit.SetBranch(key, set);
                                SetLeaf(commit, Key.Merkle(path.SliceTo(i + 1)), path.SliceFrom(i + 1), trieType);
                                return;
                            }

                            {
                                // the extension is at least 2 nibbles long
                                // 1. replace it with a branch
                                // 2. create a new, shorter extension that the branch points to
                                // 3. create a new leaf

                                var ext0Th = ext.Path[0];

                                commit.SetExtension(Key.Merkle(key.Path.AppendNibble(ext0Th, span)),
                                    ext.Path.SliceFrom(1));

                                SetLeaf(commit, Key.Merkle(path.SliceTo(i + 1)), path.SliceFrom(i + 1), trieType);

                                // Important! Make it the last as it's updating the existing key and it might affect the read value
                                commit.SetBranch(key, new NibbleSet(ext0Th, leftoverPath[0]));
                                return;
                            }
                        }

                        var lastNibblePos = ext.Path.Length - 1;
                        if (diffAt == lastNibblePos)
                        {
                            // the last nibble is different
                            // 1. trim the end of the extension.path by 1
                            // 2. add a branch at the end with nibbles set to the last and the leaf
                            // 3. add a new leaf

                            var splitAt = i + ext.Path.Length - 1;
                            var set = new NibbleSet(path[splitAt], ext.Path[lastNibblePos]);

                            commit.SetBranch(Key.Merkle(path.SliceTo(splitAt)), set);
                            SetLeaf(commit, Key.Merkle(path.SliceTo(splitAt + 1)), path.SliceFrom(splitAt + 1), trieType);

                            // Important! Make it the last as it's updating the existing key and it might affect the read value
                            commit.SetExtension(key, ext.Path.SliceTo(lastNibblePos));
                            return;
                        }

                        // the diff is not at the 0th nibble, it's not a full match as well
                        // this means that E0->B0 will turn into E1->B1->E2->B0
                        //                                             ->L0
                        var extPath = ext.Path.SliceTo(diffAt);

                        // B1
                        var branch1 = key.Path.Append(extPath, span);
                        var existingNibble = ext.Path[diffAt];
                        var addedNibble = path[i + diffAt];
                        var children = new NibbleSet(existingNibble, addedNibble);
                        commit.SetBranch(Key.Merkle(branch1), children);

                        // E2
                        var extension2 = branch1.AppendNibble(existingNibble, span);
                        if (extension2.Length < key.Path.Length + ext.Path.Length)
                        {
                            // there are some bytes to be set in the extension path, create one
                            var e2Path = ext.Path.SliceFrom(diffAt + 1);
                            commit.SetExtension(Key.Merkle(extension2), e2Path);
                        }

                        // L0
                        var leafPath = branch1.AppendNibble(addedNibble, span);
                        SetLeaf(commit, Key.Merkle(leafPath), path.SliceFrom(leafPath.Length), trieType);

                        // Important! Make it the last as it's updating the existing key
                        commit.SetExtension(key, extPath);

                        return;
                    }
                case Node.Type.Branch:
                    {
                        var nibble = path[i];

                        var childRlpRequiresUpdate = owner.IsOwnedBy(commit) == false || leftover.Length != RlpMemo.Size;
                        var memo = childRlpRequiresUpdate
                            ? RlpMemo.Decompress(leftover, branch.Children, rlpMemoWorkingSet)
                            : new RlpMemo(MakeRlpWritable(leftover));

                        memo.Clear(nibble);

                        createLeaf = !branch.Children[nibble];
                        var children = branch.Children.Set(nibble);
                        var shouldUpdateBranch = createLeaf;

                        if (shouldUpdateBranch || childRlpRequiresUpdate)
                        {
                            // Set the branch if either the children has hanged or the RLP requires the update
                            commit.SetBranch(key, children, memo.Raw);
                        }

                        if (LeafCanBeOmitted(i + 1))
                        {
#if SNAP_SYNC_SUPPORT
                            //TODO - move this to a single place
                            DataType dataType = trieType == TrieType.State ? DataType.Account : DataType.StorageCell;
                            var valueKey = Key.Raw(path, dataType, NibblePath.Empty);
                            using var readRawData = commit.Get(valueKey);
                            if (SnapSync.IsBoundaryValue(readRawData.Span))
                            {
                                NibblePath pathWithKeccak = NibblePath.Single(0, 1).Append(NibblePath.FromKey(readRawData.Span.Slice(SnapSync.PreambleLength)), stackalloc byte[33]);
                                commit.DeleteProofKey(valueKey);
                                commit.SetLeaf(Key.Merkle(path), pathWithKeccak, EntryType.UseOnce);
                                return;
                            }
#endif
                            // no need to store leaf on the last level
                            return;
                        }
                    }
                    break;
                default:
                    ThrowOutOfRange();
                    return;
            }
        }

        [DoesNotReturn]
        [StackTraceHidden]
        static void ThrowOutOfRange()
        {
            throw new ArgumentOutOfRangeException();
        }
    }

    /// <summary>
    /// Wrapper for setting merkle leaf to support snap sync. If encoded data is identified as a boundary proof value
    /// then a special type of leaf gets created with keccak encoded in its path (33 bytes). Used only for hash calc.
    /// </summary>
    /// <param name="commit"></param>
    /// <param name="key"></param>
    /// <param name="leafPath"></param>
    /// <param name="trieType"></param>
    private static void SetLeaf(ICommit commit, in Key key, NibblePath leafPath, TrieType trieType)
    {
#if SNAP_SYNC_SUPPORT
        DataType dataType = trieType == TrieType.State ? DataType.Account : DataType.StorageCell;
        var valueKey = Key.Raw(key.Path, dataType, NibblePath.Empty);
        using var readRawData = commit.Get(valueKey);
        leafPath = leafPath.GetAligned();
        if (leafPath.IsEmpty && SnapSync.IsBoundaryValue(readRawData.Span))
        {
            NibblePath pathWithKeccak = NibblePath.Single(0, 1).Append(NibblePath.FromKey(readRawData.Span.Slice(SnapSync.PreambleLength)), stackalloc byte[33]);
            commit.DeleteProofKey(valueKey);
            commit.SetLeaf(key, pathWithKeccak, EntryType.UseOnce);
            return;
        }
#endif
        commit.SetLeaf(key, leafPath);
    }

    interface IWorkItem
    {
        void DoWork(ICommit commit);
    }

    /// <summary>
    /// Builds a part of State Trie, invalidating paths and marking them as dirty whenever needed.
    /// </summary>
    private sealed class BuildStateTreeItem
    {
        private readonly CacheBudget _budget;
        private readonly BufferPool _pool;
        private readonly HashSet<Keccak> _toTouch;
        private readonly ICommit _commit;
        private Page _page;

        public BuildStateTreeItem(ICommit commit, IEnumerable<Keccak> toTouch, CacheBudget budget, BufferPool pool)
        {
            _budget = budget;
            _pool = pool;
            _toTouch = new HashSet<Keccak>(toTouch);
            _commit = commit;

            _page = _pool.Rent(false);
        }

        public void DoWork()
        {
            _commit.Visit(OnState, TrieType.State);

            // dirty the leftovers
            foreach (var keccak in _toTouch)
            {
                // Requires checking whether exists or not. There are cases where Storage Tries are
                // created and cleaned  during one transaction and require asserting that the value is not there.
                // This also creates a dependency on storage roots computation.
                var key = Key.Account(keccak);
                using var value = _commit.Get(key);

                if (value.IsEmpty)
                {
                    Delete(in key.Path, 0, _commit, _budget);
                }
                else
                {
                    MarkPathDirty(in key.Path, _page.Span, _commit, _budget, TrieType.State);
                }
            }

            _pool.Return(_page);
            _page = default;
        }

        private void OnState(in Key key, ReadOnlySpan<byte> value)
        {
            Debug.Assert(key.Type == DataType.Account);

            if (value.IsEmpty)
            {
                Delete(in key.Path, 0, _commit!, _budget);
            }
            else
            {
                MarkPathDirty(in key.Path, _page.Span, _commit!, _budget, TrieType.State);
            }

            // mark as touched already
            _toTouch.Remove(key.Path.UnsafeAsKeccak);
        }
    }

    private sealed class BuildStorageTriesItem : IWorkItem
    {
        private readonly ComputeMerkleBehavior _behavior;
        private readonly ICommit _parent;
        private readonly Keccak _account;
        private readonly CacheBudget _budget;
        private readonly BufferPool _pool;
        private PrefixingCommit? _prefixed;
        private Page _page;

        public BuildStorageTriesItem(ComputeMerkleBehavior behavior, ICommit parent, Keccak account,
            CacheBudget budget, BufferPool pool)
        {
            _behavior = behavior;
            _parent = parent;
            _account = account;
            _budget = budget;
            _pool = pool;
            _prefixed = null;

            _page = pool.Rent(false);
        }

        public void DoWork(ICommit commit)
        {
            try
            {
                _prefixed = new PrefixingCommit(commit);
                _prefixed.SetPrefix(_account);
                _parent.Visit(OnStorage, TrieType.Storage);

                CalculateStorageRoot(_account, _behavior, _budget, _prefixed, commit);
            }
            finally
            {
                _pool.Return(_page);
                _page = default;
            }
        }

        private void OnStorage(in Key key, ReadOnlySpan<byte> value)
        {
            Debug.Assert(key.Type == DataType.StorageCell);

            var keccak = key.Path.UnsafeAsKeccak;
            if (_account != keccak)
            {
                return;
            }

            if (value.IsEmpty)
            {
                Delete(in key.StoragePath, 0, _prefixed!, _budget);
            }
            else
            {
                MarkPathDirty(in key.StoragePath, _page.Span, _prefixed!, _budget, TrieType.Storage);
            }
        }

        public static void CalculateStorageRoot(in Keccak keccak, ComputeMerkleBehavior behavior, CacheBudget budget,
            PrefixingCommit prefixed, ICommit commit)
        {
            // Don't parallelize this work as it would be counter-productive to have parallel over parallel.
            const ComputeHint hint = ComputeHint.DontUseParallel;

            // compute new storage root hash
            UIntPtr stack = default;
            using var ctx = new ComputeContext(prefixed, TrieType.Storage, hint, budget, behavior._pool, ref stack);
            behavior.Compute(Key.Merkle(NibblePath.Empty), ctx, out var keccakOrRlp, out _);

            // Read the existing account from the commit, without the prefix as accounts are not prefixed
            var key = Key.Account(keccak);
            using var accountOwner = commit.Get(key);

            if (accountOwner.IsEmpty == false)
            {
                Account.ReadFrom(accountOwner.Span, out var account);

                // update it
                account.WithChangedStorageRoot(keccakOrRlp.Keccak, out account);

                // set it in
                using var pooled = ctx.Rent();
                commit.Set(key, account.WriteTo(pooled.Span));
            }
            else
            {
                //see: https://sepolia.etherscan.io/tx/0xb3790025b59b7e31d6d8249e8962234217e0b5b02e47ecb2942b8c4d0f4a3cfe
                // Contract is created and destroyed, then its values are destroyed
                // The storage root should be empty, otherwise, it's wrong
                //TODO: remove assert for fast sync / healing implementation - storage tries saved before accounts
                //Debug.Assert(keccakOrRlp.Keccak == Keccak.EmptyTreeHash,
                //    $"Non-existent account with hash of {keccak.ToString()} should have the storage root empty");
            }
        }
    }


    public bool CanPrefetch => true;

    public void Prefetch(in Keccak account, IPrefetcherContext context)
    {
        PrefetchImpl(account, Unsafe.NullRef<Keccak>(), context);
    }

    public void Prefetch(in Keccak account, in Keccak storage, IPrefetcherContext context)
    {
        PrefetchImpl(account, storage, context);
    }

    [SkipLocalsInit]
    private static void PrefetchImpl(in Keccak account, in Keccak storage, IPrefetcherContext context)
    {
        var isAccountPrefetch = Unsafe.IsNullRef(in storage);
        var accountPath = NibblePath.FromKey(account);

        // Use a similar algorithm to walking through as the MarkPathAsDirty.
        // Preload only branches
        // Flag forcing the leaf creation, that saves one get of the non-existent value.
        var path = NibblePath.FromKey(isAccountPrefetch ? account : storage);

        for (var i = 0; i <= path.Length; i++)
        {
            var slice = path.SliceTo(i);
            var key = isAccountPrefetch ? Key.Merkle(slice) : Key.Raw(accountPath, DataType.Merkle, slice);
            var leftoverPath = path.SliceFrom(i);

            // Query for the node
            using var owner = context.Get(key);
            if (owner.IsEmpty)
            {
                // A leaf will be created here.
                return;
            }

            // read the existing one
            Node.ReadFrom(out var type, out _, out var ext, out var branch, owner.Span);

            var nonLocal = owner.QueryDepth > 0;

            switch (type)
            {
                case Node.Type.Leaf:
                    if (nonLocal)
                    {
                        // data came from the depth
                        context.Set(key, owner.Span, EntryType.UseOnce);
                    }
                    return;
                case Node.Type.Extension:
                    {
                        if (nonLocal)
                        {
                            // data came from the depth
                            context.Set(key, owner.Span, EntryType.UseOnce);
                        }

                        var diffAt = ext.Path.FindFirstDifferentNibble(leftoverPath);
                        if (diffAt == ext.Path.Length)
                        {
                            // The path overlaps with what is there, move forward
                            i += ext.Path.Length - 1;

                            // Consider adding the extension here?
                            continue;
                        }

                        // The paths are different, handle by MarkPathAsDirty
                        return;
                    }
                case Node.Type.Branch:
                    if (nonLocal)
                    {
                        // Will be modified and we can set to persistent already
                        context.Set(key, owner.Span, EntryType.Persistent);
                    }

                    var nibble = path[i];
                    if (branch.Children[nibble] == false)
                    {
                        // no children set, will be created
                        return;
                    }

                    if (LeafCanBeOmitted(i + 1))
                    {
                        // no need to store leaf on the last level
                        return;
                    }

                    break;
                default:
                    return;
            }
        }
    }

    public void Dispose()
    {
        _meter.Dispose();
    }
}
