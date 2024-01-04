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
using Paprika.Utils;

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
    /// <summary>
    /// The upper boundary of memory needed to write RLP of any Merkle node.
    /// </summary>
    /// <remarks>
    /// Actually, it is lower, ~600 but let's wrap it nicely.
    /// </remarks>
    private const int MaxBufferNeeded = 1024;

    public const int DefaultMinimumTreeLevelToMemoizeKeccak = 2;
    public const int MemoizeKeccakEveryNLevel = 2;

    private readonly int _minimumTreeLevelToMemoizeKeccak;
    private readonly int _memoizeKeccakEvery;
    private readonly bool _memoizeRlp;
    private readonly bool _useParallel;
    private readonly Meter _meter;
    private readonly Histogram<long> _storageProcessing;
    private readonly Histogram<long> _stateProcessing;
    private readonly Histogram<long> _totalMerkle;

    private readonly BufferPool _pool = new(1024, true, "Merkle");

    /// <summary>
    /// Initializes the Merkle.
    /// </summary>
    /// <param name="minimumTreeLevelToMemoizeKeccak">Minimum lvl of the tree to memoize the Keccak of a branch node.</param>
    /// <param name="memoizeKeccakEvery">How often (which lvl mod) should Keccaks be memoized.</param>
    /// <param name="memoizeRlp">Whether the RLP of branches should be memoized in memory (but not stored)
    /// to limit the number of lookups for the children and their Keccak recalculation.
    /// This includes invalidating the memoized RLP whenever a path that it caches is marked as dirty.</param>
    public ComputeMerkleBehavior(int minimumTreeLevelToMemoizeKeccak = DefaultMinimumTreeLevelToMemoizeKeccak,
        int memoizeKeccakEvery = MemoizeKeccakEveryNLevel,
        bool memoizeRlp = true,
        bool useParallel = true)
    {
        _minimumTreeLevelToMemoizeKeccak = minimumTreeLevelToMemoizeKeccak;
        _memoizeKeccakEvery = memoizeKeccakEvery;
        _memoizeRlp = memoizeRlp;
        _useParallel = useParallel;

        _meter = new Meter("Paprika.Merkle");
        _storageProcessing = _meter.CreateHistogram<long>("State processing", "ms",
            "How long it takes to process state");
        _stateProcessing = _meter.CreateHistogram<long>("Storage processing", "ms",
            "How long it takes to process storage");
        _totalMerkle = _meter.CreateHistogram<long>("Total Merkle", "ms",
            "How long it takes to process Merkle total");
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
        var value = Compute(in root,
            new ComputeContext(wrapper, TrieType.State, hint, CacheBudget.Options.None.Build()));
        return new Keccak(value.Span);
    }

    public Keccak CalculateStorageHash(IReadOnlyWorldState commit, in Keccak account, NibblePath storagePath = default)
    {
        const ComputeHint hint = ComputeHint.DontUseParallel | ComputeHint.SkipCachedInformation;
        var prefixed = new PrefixingCommit(new CommitWrapper(commit));
        prefixed.SetPrefix(account);

        var root = Key.Merkle(storagePath);
        var value = Compute(in root,
            new ComputeContext(prefixed, TrieType.Storage, hint, CacheBudget.Options.None.Build()));
        return new Keccak(value.Span);
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

        public void Set(in Key key, in ReadOnlySpan<byte> payload)
        {
            // NOP
        }

        public void Set(in Key key, in ReadOnlySpan<byte> payload0, in ReadOnlySpan<byte> payload1)
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

    public Keccak BeforeCommit(ICommit commit, CacheBudget budget) => BeforeCommit(commit, budget, false);

    public Keccak BeforeCommit(ICommit commit, CacheBudget budget, bool skipRootCalculation)
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
            ScatterGather(commit, GetStorageWorkItems(commit, budget));
        }

        using (_stateProcessing.Measure())
        {
            using var cached = commit.WriteThroughCacheRoot(ShouldCacheKey, ShouldCacheResult, _pool);

            new BuildStateTreeItem(cached, commit.Stats.Keys, budget).DoWork();

            cached.SealCaching();

            if (skipRootCalculation)
                return Keccak.Zero;

            var root = Key.Merkle(NibblePath.Empty);
            var rootKeccak = Compute(root, new ComputeContext(cached, TrieType.State, ComputeHint.None, budget));

            Debug.Assert(rootKeccak.DataType == KeccakOrRlp.Type.Keccak);

            var value = new Keccak(rootKeccak.Span);
            RootHash = value;

            return value;
        }
    }

    /// <summary>
    /// Runs the work items in parallel then gathers the data and commits to the parent.
    /// </summary>
    /// <param name="commit">The original commit.</param>
    /// <param name="workItems">The work items.</param>
    private void ScatterGather(ICommit commit, IEnumerable<IWorkItem> workItems)
    {
        if (_useParallel)
        {
            var children = new ConcurrentQueue<IChildCommit>();

            Parallel.ForEach(workItems,
                () => commit.GetChild().WriteThroughCacheChild(ShouldCacheKey, ShouldCacheResult, _pool),
                (workItem, _, child) =>
                {
                    workItem.DoWork(child);
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
        else
        {
            using var child = commit.GetChild().WriteThroughCacheChild(ShouldCacheKey, ShouldCacheResult, _pool);
            foreach (var workItem in workItems)
            {
                workItem.DoWork(child);
            }
            child.Commit();
        }
    }

    private static bool ShouldCacheKey(in Key key) => key.Type == DataType.Merkle;
    private static bool ShouldCacheResult(in ReadOnlySpanOwnerWithMetadata<byte> result) => true;

    /// <summary>
    /// Builds works items responsible for building up the storage tries.
    /// </summary>
    private IEnumerable<IWorkItem> GetStorageWorkItems(ICommit commit, CacheBudget budget)
    {
        var sum = commit.Stats.Sum(pair => pair.Value);

        // make 2 more batches than CPU count to allow some balancing
        var batchBudget = sum / (Environment.ProcessorCount * 2);

        var list = new List<HashSet<Keccak>>();
        var current = new HashSet<Keccak>();
        var currentSize = 0;

        foreach (var (key, count) in commit.Stats)
        {
            if (count > 0)
            {
                current.Add(key);
                currentSize += count;

                if (currentSize > batchBudget)
                {
                    list.Add(current);
                    currentSize = 0;
                    current = new HashSet<Keccak>();
                }
            }
        }

        if (current.Count > 0)
            list.Add(current);

        return list.Select(set => new BuildStorageTriesItem(this, commit, set, budget)).ToArray();
    }

    public ReadOnlySpan<byte> InspectBeforeApply(in Key key, ReadOnlySpan<byte> data)
    {
        return data;
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

    private readonly struct ComputeContext
    {
        public readonly ICommit Commit;
        public readonly TrieType TrieType;
        public readonly ComputeHint Hint;
        public readonly CacheBudget Budget;

        public ComputeContext(ICommit commit, TrieType trieType, ComputeHint hint, CacheBudget budget)
        {
            Commit = commit;
            TrieType = trieType;
            Hint = hint;
            Budget = budget;
        }
    }

    private KeccakOrRlp Compute(scoped in Key key, scoped in ComputeContext ctx)
    {
        using var owner = ctx.Commit.Get(key);

        if (owner.IsEmpty)
        {
            // empty tree, return empty
            return Keccak.EmptyTreeHash;
        }

        var leftover = Node.ReadFrom(owner.Span, out var type, out var leaf, out var ext, out var branch);
        switch (type)
        {
            case Node.Type.Leaf:
                return EncodeLeaf(key, leaf, ctx);
            case Node.Type.Extension:
                return EncodeExtension(key, ext, ctx);
            case Node.Type.Branch:
                var useMemoized = !ctx.Hint.HasFlag(ComputeHint.SkipCachedInformation);
                if (useMemoized && branch.HasKeccak)
                {
                    // return memoized value
                    return branch.Keccak;
                }

                return EncodeBranch(key, branch, leftover, owner.IsOwnedBy(ctx.Commit), ctx);
            default:
                throw new ArgumentOutOfRangeException();
        }
    }

    private KeccakOrRlp EncodeLeaf(scoped in Key key, scoped in Node.Leaf leaf, scoped in ComputeContext ctx)
    {
        var leafPath =
            key.Path.Append(leaf.Path, stackalloc byte[key.Path.MaxByteLength + leaf.Path.MaxByteLength + 1]);

        var leafKey = ctx.TrieType == TrieType.State
            ? Key.Account(leafPath)
            // the prefix will be added by the prefixing commit
            : Key.Raw(leafPath, DataType.StorageCell, NibblePath.Empty);

        using var leafData = ctx.Commit.Get(leafKey);

        KeccakOrRlp keccakOrRlp;
        if (ctx.TrieType == TrieType.State)
        {
            Account.ReadFrom(leafData.Span, out var account);

            if (ctx.Hint.HasFlag(ComputeHint.ForceStorageRootHashRecalculation))
            {
                var prefixed = new PrefixingCommit(ctx.Commit);
                prefixed.SetPrefix(leafKey.Path);
                var ctx2 = new ComputeContext(prefixed, TrieType.Storage, ctx.Hint, ctx.Budget);
                var storageRoot = Compute(Key.Merkle(NibblePath.Empty), ctx2);
                account = new Account(account.Balance, account.Nonce, account.CodeHash, new Keccak(storageRoot.Span));
            }

            Node.Leaf.KeccakOrRlp(leaf.Path, account, out keccakOrRlp);
            return keccakOrRlp;
        }

        Debug.Assert(ctx.TrieType == TrieType.Storage, "Only accounts now");

        Node.Leaf.KeccakOrRlp(leaf.Path, leafData.Span, out keccakOrRlp);
        return keccakOrRlp;
    }

    private KeccakOrRlp EncodeBranch(scoped in Key key, scoped in Node.Branch branch, ReadOnlySpan<byte> previousRlp,
        bool isOwnedByCommit, scoped in ComputeContext ctx)
    {
        // Parallelize at the root level any trie, state or storage, that have all children set.
        // This heuristic is used to estimate that the tree should be big enough to gain from making this computation
        // parallel but without calculating and storing additional information how big is the tree.
        var runInParallel = !ctx.Hint.HasFlag(ComputeHint.DontUseParallel) &&
                            key.Path.IsEmpty && // only for root
                            branch.Children.AllSet && // only where all children set
                            branch.Leafs.IsEmpty; // only if no embedded leafs

        var memoize = !ctx.Hint.HasFlag(ComputeHint.SkipCachedInformation) && ShouldMemoizeBranchRlp(key.Path);
        var bytes = ArrayPool<byte>.Shared.Rent(MaxBufferNeeded);

        byte[]? rlpMemoization = null;
        RlpMemo memo = default;

        if (memoize)
        {
            Span<byte> data;

            if (isOwnedByCommit && previousRlp.IsEmpty == false)
            {
                data = MakeRlpWritable(previousRlp);
            }
            else
            {
                rlpMemoization = ArrayPool<byte>.Shared.Rent(RlpMemo.Size);
                data = rlpMemoization.AsSpan(0, RlpMemo.Size);
                if (previousRlp.IsEmpty)
                {
                    data.Clear();
                }
                else
                {
                    previousRlp.CopyTo(data);
                }
            }

            memo = new RlpMemo(data);
        }

        // leave for length preamble
        const int initialShift = Rlp.MaxLengthOfLength + 1;
        var stream = new RlpStream(bytes)
        {
            Position = initialShift
        };

        if (!runInParallel)
        {
            const int additionalBytesForNibbleAppending = 1;
            Span<byte> childSpan = stackalloc byte[key.Path.MaxByteLength + additionalBytesForNibbleAppending];

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

                    KeccakOrRlp value;
                    if (branch.Leafs.TryGetLeaf(i, key.Path, out scoped var leaf))
                    {
                        // embedded leaf
                        value = EncodeLeaf(Key.Merkle(childPath), leaf, ctx);
                    }
                    else
                    {
                        value = Compute(Key.Merkle(childPath), ctx);
                    }

                    if (value.DataType == KeccakOrRlp.Type.Keccak)
                    {
                        stream.Encode(value.Span);
                    }
                    else
                    {
                        stream.Write(value.Span);
                    }

                    if (memoize) memo.Set(value, i);
                }
                else
                {
                    stream.EncodeEmptyArray();

                    // TODO: might be not needed
                    if (memoize) memo.Clear(i);
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
            Parallel.For((long)0, NibbleSet.NibbleCount, nibble =>
            {
                var childPath = NibblePath.FromKey(stackalloc byte[1] { (byte)(nibble << NibblePath.NibbleShift) }, 0)
                    .SliceTo(1);
                var child = commits[nibble] = commit.GetChild();
                results[nibble] = Compute(Key.Merkle(childPath), new(child, trieType, hint, budget)).Span.ToArray();
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
                if (memoize)
                {
                    if (value.Length == Keccak.Size)
                    {
                        memo.SetRaw(value, i);
                    }
                    else
                    {
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

        var result = KeccakOrRlp.FromSpan(bytes.AsSpan(from, end - from));

        ArrayPool<byte>.Shared.Return(bytes);

        if (result.DataType == KeccakOrRlp.Type.Keccak)
        {
            // Memoize only if Keccak and falls into the criteria.
            // Storing RLP for an embedded node is useless as it can be easily re-calculated.
            if (ShouldMemoizeBranchKeccak(key.Path))
            {
                ctx.Commit.SetBranch(key, branch.Children, new Keccak(result.Span), branch.Leafs, memo.Raw);
            }
            else
            {
                ctx.Commit.SetBranch(key, branch.Children, branch.Leafs, memo.Raw);
            }
        }

        if (rlpMemoization != null)
            ArrayPool<byte>.Shared.Return(rlpMemoization);

        return result;
    }

    /// <summary>
    /// Makes the RLP writable. Should be used only after ensuring that the current <see cref="ICommit"/>
    /// is the owner of the span. This can be done by using <see cref="ReadOnlySpanOwner{T}.IsOwnedBy"/>.
    /// </summary>
    private static Span<byte> MakeRlpWritable(ReadOnlySpan<byte> previousRlp) =>
        MemoryMarshal.CreateSpan(ref MemoryMarshal.GetReference(previousRlp), previousRlp.Length);

    private bool ShouldMemoizeBranchRlp(in NibblePath branchPath)
    {
        return _memoizeRlp &&
               branchPath.Length >= 1; // a simple condition to memoize only more nested RLPs
    }

    private bool ShouldMemoizeBranchKeccak(in NibblePath branchPath)
    {
        var level = branchPath.Length - _minimumTreeLevelToMemoizeKeccak;

        // memoize only if the branch is deeper than _minimumTreeLevelToMemoizeKeccak and every _memoizeKeccakEvery
        return level >= 0 && level % _memoizeKeccakEvery == 0;
    }

    private KeccakOrRlp EncodeExtension(scoped in Key key, scoped in Node.Extension ext, scoped in ComputeContext ctx)
    {
        Span<byte> span = stackalloc byte[Math.Max(ext.Path.HexEncodedLength, key.Path.MaxByteLength + 1)];

        // retrieve the children keccak-or-rlp
        var branchKeccakOrRlp = Compute(Key.Merkle(key.Path.Append(ext.Path, span)), ctx);

        ext.Path.HexEncode(span, false);
        span = span.Slice(0, ext.Path.HexEncodedLength); // trim the span to the hex

        var contentLength = Rlp.LengthOf(span) + (branchKeccakOrRlp.DataType == KeccakOrRlp.Type.Rlp
            ? branchKeccakOrRlp.Span.Length
            : Rlp.LengthOfKeccakRlp);

        var totalLength = Rlp.LengthOfSequence(contentLength);

        RlpStream stream = new(stackalloc byte[totalLength]);
        stream.StartSequence(contentLength);
        stream.Encode(span);
        stream.Encode(branchKeccakOrRlp.Span);

        return stream.ToKeccakOrRlp();
    }

    /// <summary>
    /// This component appends the prefix to all the commit operations.
    /// It's useful for storage operations, that have their key prefixed with the account.
    /// </summary>
    private class PrefixingCommit : ICommit
    {
        private readonly ICommit _commit;
        private Keccak _keccak;

        public PrefixingCommit(ICommit commit)
        {
            _commit = commit;
        }

        public void SetPrefix(in NibblePath path) => SetPrefix(path.UnsafeAsKeccak);

        public void SetPrefix(in Keccak keccak) => _keccak = keccak;

        public ReadOnlySpanOwnerWithMetadata<byte> Get(scoped in Key key) => _commit.Get(Build(key));

        public void Set(in Key key, in ReadOnlySpan<byte> payload) => _commit.Set(Build(key), in payload);

        public void Set(in Key key, in ReadOnlySpan<byte> payload0, in ReadOnlySpan<byte> payload1)
            => _commit.Set(Build(key), payload0, payload1);

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

            public ReadOnlySpanOwnerWithMetadata<byte> Get(scoped in Key key) => _commit.Get(_parent.Build(key));

            public void Set(in Key key, in ReadOnlySpan<byte> payload) => _commit.Set(_parent.Build(key), payload);

            public void Set(in Key key, in ReadOnlySpan<byte> payload0, in ReadOnlySpan<byte> payload1) =>
                _commit.Set(_parent.Build(key), payload0, payload1);

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
        var leftover = Node.ReadFrom(owner.Span, out var type, out var leaf, out var ext, out var branch);
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
                        // The node has not change its type
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

                    DeleteStatus status;
                    var potentialLeafPath = path.SliceFrom(at);
                    scoped var embedded = branch.Leafs;

                    if (branch.Leafs.TryGetLeafWithSameNibble(potentialLeafPath, out var embeddedLeaf))
                    {
                        if (embeddedLeaf.Path.Equals(potentialLeafPath))
                        {
                            embedded = branch.Leafs.Remove(potentialLeafPath, stackalloc byte[branch.Leafs.MaxByteSize]);

                            // this is a leaf to remove
                            status = DeleteStatus.LeafDeleted;
                        }
                        else
                        {
                            // child reports non-existence
                            return DeleteStatus.KeyDoesNotExist;
                        }
                    }
                    else
                    {
                        var newAt = at + 1;
                        status = Delete(path, newAt, commit, budget);
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
                            if (status == DeleteStatus.BranchToLeafOrExtension)
                            {
                                // Handle branchToLeaf in a different way by checking what is underneath.
                                if (TryInlineChild(commit, path, branch, key, at))
                                {
                                    // the node is inline and the branch is updated, return

                                    return DeleteStatus.NodeTypePreserved;
                                }
                            }

                            // if either has the keccak or has the leftover rlp, clean
                            if (branch.HasKeccak)
                            {
                                // reset
                                commit.SetBranch(key, branch.Children, embedded);
                            }

                            return DeleteStatus.NodeTypePreserved;
                        }
                    }

                    Debug.Assert(status == DeleteStatus.LeafDeleted, "leaf deleted");

                    var children = branch.Children.Remove(nibble);

                    // if branch has still more than one child, just update the set
                    if (children.SetCount > 1)
                    {
                        commit.SetBranch(key, children, embedded);
                        return DeleteStatus.NodeTypePreserved;
                    }

                    // there's an only child now. The branch should be collapsed
                    if (embedded.Count == 1)
                    {
                        commit.SetLeaf(key, embedded.GetSingleLeaf(leftoverPath));
                        return DeleteStatus.BranchToLeafOrExtension;
                        // there's a single child that is an embedded leaf
                    }

                    var onlyNibble = children.SmallestNibbleSet;
                    var onlyChildPath = slice.AppendNibble(onlyNibble,
                        stackalloc byte[slice.MaxByteLength + 1]);

                    var onlyChildKey = Key.Merkle(onlyChildPath);
                    using var onlyChildSpanOwner = commit.Get(onlyChildKey);

                    // need to collapse the branch
                    Node.ReadFrom(onlyChildSpanOwner.Span, out var childType, out var childLeaf, out var childExt,
                        out _);

                    var firstNibblePath =
                        NibblePath
                            .FromKey(stackalloc byte[1] { (byte)(onlyNibble << NibblePath.NibbleShift) })
                            .SliceTo(1);

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

                    // The leaf node might be created only on the collapse of the child
                    var leafPath = firstNibblePath.Append(childLeaf.Path, stackalloc byte[NibblePath.FullKeccakByteLength]);

                    // replace branch with the leaf
                    commit.SetLeaf(key, leafPath);

                    // delete the only child
                    commit.DeleteKey(onlyChildKey);

                    return DeleteStatus.BranchToLeafOrExtension;
                }
            default:
                throw new ArgumentOutOfRangeException();
        }

        [MethodImpl(MethodImplOptions.NoInlining)]
        static bool TryInlineChild(ICommit commit, in NibblePath path, in Node.Branch branch, in Key branchKey, int at)
        {
            var childPath = path.SliceTo(at + 1);
            var childKey = Key.Merkle(childPath);

            scoped NibblePath concatenated;
            using (var child = commit.Get(childKey))
            {
                Node.ReadFrom(child.Span, out var ct, out var l, out _, out _);
                if (ct != Node.Type.Leaf)
                {
                    return false;
                }

                concatenated = childPath.Append(l.Path, stackalloc byte[NibblePath.FullKeccakByteLength]);
            }

            var newLeaf = concatenated.SliceFrom(at);
            var newLeafs = branch.Leafs.Add(newLeaf, stackalloc byte[branch.Leafs.SpanSizeForGrow]);

            commit.SetBranch(branchKey, branch.Children, newLeafs);
            commit.DeleteKey(childKey);

            return true;
        }
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

        Node.ReadFrom(copy, out var childType, out var childLeaf, out var childExt, out _);

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

    private static void MarkPathDirty(in NibblePath path, ICommit commit, CacheBudget budget)
    {
        Span<byte> span = stackalloc byte[64];

        for (var i = 0; i <= path.Length; i++)
        {
            var slice = path.SliceTo(i);
            var key = Key.Merkle(slice);

            var leftoverPath = path.SliceFrom(i);

            using var owner = commit.Get(key);

            if (owner.IsEmpty)
            {
                // no value set now, create one
                commit.SetLeaf(key, leftoverPath);
                return;
            }

            // read the existing one
            var leftover = Node.ReadFrom(owner.Span, out var type, out var leaf, out var ext, out var branch);
            switch (type)
            {
                case Node.Type.Leaf:
                    {
                        var diffAt = leaf.Path.FindFirstDifferentNibble(leftoverPath);

                        if (diffAt == leaf.Path.Length)
                        {
                            // This is update in place. The parent is marked as parent as dirty.
                            // The structure of Merkle does not change.
                            return;
                        }

                        var nibbleA = leaf.Path[diffAt];
                        var nibbleB = leftoverPath[diffAt];

                        // Important! Make it the last in set of changes as it may be updating the key that was read (leaf)
                        if (diffAt > 0)
                        {
                            // diff is not on the 0th position, so it will be a branch but preceded with an extension
                            commit.SetExtension(key, leftoverPath.SliceTo(diffAt));
                        }

                        // Create branch with embedded leafs
                        var branchKey = Key.Merkle(path.SliceTo(i + diffAt));
                        var embedded = new EmbeddedLeafs(leaf.Path.SliceFrom(diffAt), leftoverPath.SliceFrom(diffAt), span);
                        commit.SetBranch(branchKey, new NibbleSet(nibbleA, nibbleB), embedded);

                        return;
                    }
                case Node.Type.Extension:
                    {
                        var diffAt = ext.Path.FindFirstDifferentNibble(leftoverPath);
                        if (diffAt == ext.Path.Length)
                        {
                            // the path overlaps with what is there, move forward
                            i += ext.Path.Length - 1;
                            continue;
                        }

                        if (diffAt == 0)
                        {
                            if (ext.Path.Length == 1)
                            {
                                // special case of an extension being only 1 nibble long
                                // 1. replace an extension with a branch
                                // 2. leave the next branch as is
                                // 3. embed the leaf in the branch
                                var set = new NibbleSet(ext.Path[0], leftoverPath[0]);
                                commit.SetBranch(key, set, new EmbeddedLeafs(leftoverPath, span));
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

                                var leafs = new EmbeddedLeafs(path.SliceFrom(i), span);

                                // Important! Make it the last as it's updating the existing key and it might affect the read value
                                commit.SetBranch(key, new NibbleSet(ext0Th, leftoverPath[0]), leafs);
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

                            var leafs = new EmbeddedLeafs(path.SliceFrom(splitAt), span);
                            commit.SetBranch(Key.Merkle(path.SliceTo(splitAt)), set, leafs);

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

                        // LO as embedded
                        var leafPathLength = branch1.Length;
                        Span<byte> leafSpan = stackalloc byte[EmbeddedLeafs.PathSpanSize(1)];
                        commit.SetBranch(Key.Merkle(branch1), children,
                            new EmbeddedLeafs(path.SliceFrom(leafPathLength), leafSpan));

                        // E2
                        Span<byte> ext2Span = stackalloc byte[branch1.MaxByteLength + 1];
                        var extension2 = branch1.AppendNibble(existingNibble, ext2Span);
                        if (extension2.Length < key.Path.Length + ext.Path.Length)
                        {
                            // there are some bytes to be set in the extension path, create one
                            var e2Path = ext.Path.SliceFrom(diffAt + 1);
                            commit.SetExtension(Key.Merkle(extension2), e2Path);
                        }

                        // L0 is embedded above

                        // Important! Make it the last as it's updating the existing key
                        commit.SetExtension(key, extPath);

                        return;
                    }
                case Node.Type.Branch:
                    {
                        var nibble = path[i];

                        var leafPath = path.SliceFrom(i);

                        if (branch.Children[nibble])
                        {
                            // The child exists. Check if there's a leaf for it
                            if (branch.Leafs.TryGetLeafWithSameNibble(leafPath, out var existingLeaf))
                            {
                                if (!existingLeaf.Path.Equals(leafPath))
                                {
                                    var diffAt = existingLeaf.Path.FindFirstDifferentNibble(leafPath);
                                    if (diffAt > 1)
                                    {
                                        var extKey = Key.Merkle(key.Path.AppendNibble(leafPath.FirstNibble, span));
                                        commit.SetExtension(extKey, leafPath.SliceFrom(1).SliceTo(diffAt - 1));
                                    }

                                    var branchKey = key.Path.Append(leafPath.SliceTo(diffAt), span);
                                    var set = new NibbleSet(leafPath[diffAt], existingLeaf.Path[diffAt]);
                                    var leafs = new EmbeddedLeafs(leafPath.SliceFrom(diffAt),
                                        existingLeaf.Path.SliceFrom(diffAt),
                                        stackalloc byte[EmbeddedLeafs.PathSpanSize(2)]);

                                    // set new branch
                                    commit.SetBranch(Key.Merkle(branchKey), set, leafs);

                                    // update the existing by removing the embedded child
                                    commit.SetBranch(key, branch.Children,
                                        branch.Leafs.Remove(existingLeaf.Path,
                                            stackalloc byte[branch.Leafs.SpanSizeForShrink]));
                                }

                                return;
                            }

                            // No embedded leaf with the same first nibble. It must be an extension or a branch, proceed
                            // Check if there are some memoized data. If they are, clear them and set the branch. 
                            if (branch.HasKeccak)
                            {
                                commit.SetBranch(key, branch.Children, branch.Leafs);
                            }
                        }
                        else
                        {
                            // The child was not set yet, create and embed one
                            var children = branch.Children.Set(nibble);

                            var leafBytes = ArrayPool<byte>.Shared.Rent(EmbeddedLeafs.MaxWorksetNeeded);
                            var leafs = branch.Leafs.Add(leafPath, leafBytes);

                            commit.SetBranch(key, children, leafs);

                            // return leafs
                            ArrayPool<byte>.Shared.Return(leafBytes);

                            // branch is set
                            return;
                        }
                    }
                    break;
                default:
                    throw new ArgumentOutOfRangeException();
            }
        }
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
        private readonly HashSet<Keccak> _toTouch;
        private readonly ICommit _commit;

        public BuildStateTreeItem(ICommit commit, IEnumerable<Keccak> toTouch, CacheBudget budget)
        {
            _budget = budget;
            _toTouch = new HashSet<Keccak>(toTouch);
            _commit = commit;
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
                    MarkPathDirty(in key.Path, _commit, _budget);
                }
            }
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
                MarkPathDirty(in key.Path, _commit!, _budget);
            }

            // mark as touched already
            _toTouch.Remove(key.Path.UnsafeAsKeccak);
        }
    }

    private sealed class BuildStorageTriesItem : IWorkItem
    {
        private readonly ComputeMerkleBehavior _behavior;
        private readonly ICommit _parent;
        private readonly HashSet<Keccak> _accounts;
        private readonly CacheBudget _budget;
        private PrefixingCommit? _prefixed;

        public BuildStorageTriesItem(ComputeMerkleBehavior behavior, ICommit parent, HashSet<Keccak> accounts,
            CacheBudget budget)
        {
            _behavior = behavior;
            _parent = parent;
            _accounts = accounts;
            _budget = budget;
            _prefixed = null;
        }

        public void DoWork(ICommit commit)
        {
            _prefixed = new PrefixingCommit(commit);
            _parent.Visit(OnStorage, TrieType.Storage);
            CalculateStorageRoots(commit);
        }

        private void OnStorage(in Key key, ReadOnlySpan<byte> value)
        {
            Debug.Assert(key.Type == DataType.StorageCell);

            var keccak = key.Path.UnsafeAsKeccak;
            if (_accounts.Contains(keccak) == false)
            {
                return;
            }

            _prefixed!.SetPrefix(keccak);

            if (value.IsEmpty)
            {
                Delete(in key.StoragePath, 0, _prefixed, _budget);
            }
            else
            {
                MarkPathDirty(in key.StoragePath, _prefixed, _budget);
            }
        }

        private void CalculateStorageRoots(ICommit commit)
        {
            // Don't parallelize this work as it would be counter-productive to have parallel over parallel.
            const ComputeHint hint = ComputeHint.DontUseParallel;

            Span<byte> accountSpan = stackalloc byte[Account.MaxByteCount];

            foreach (var accountAddress in _accounts)
            {
                _prefixed.SetPrefix(accountAddress);

                // compute new storage root hash
                var keccakOrRlp = _behavior.Compute(Key.Merkle(NibblePath.Empty),
                    new(_prefixed, TrieType.Storage, hint, _budget));
                var storageRoot = new Keccak(keccakOrRlp.Span);

                // read the existing account
                var key = Key.Account(accountAddress);
                using var accountOwner = commit.Get(key);

                if (accountOwner.IsEmpty == false)
                {
                    Account.ReadFrom(accountOwner.Span, out var account);

                    // update it
                    account = account.WithChangedStorageRoot(storageRoot);

                    // set it
                    commit.Set(key, account.WriteTo(accountSpan));
                }
                else
                {
                    //see: https://sepolia.etherscan.io/tx/0xb3790025b59b7e31d6d8249e8962234217e0b5b02e47ecb2942b8c4d0f4a3cfe
                    // Contract is created and destroyed, then its values are destroyed
                    // The storage root should be empty, otherwise, it's wrong
                    Debug.Assert(storageRoot == Keccak.EmptyTreeHash,
                        $"Non-existent account with hash of {accountAddress.ToString()} should have the storage root empty");
                }
            }
        }
    }

    public void Dispose()
    {
        _pool.Dispose();
        _meter.Dispose();
    }
}