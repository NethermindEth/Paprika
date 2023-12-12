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

    private readonly bool _fullMerkle;
    private readonly int _minimumTreeLevelToMemoizeKeccak;
    private readonly int _memoizeKeccakEvery;
    private readonly bool _memoizeRlp;
    private readonly Meter _meter;
    private readonly Histogram<long> _stateRootHashCompute;
    private readonly Histogram<long> _storageRootsHashCompute;
    private readonly Histogram<long> _totalMerkle;
    private readonly Histogram<long> _trieBuilding;

    /// <summary>
    /// Initializes the Merkle.
    /// </summary>
    /// <param name="fullMerkle">Whether run a full Merkle protocol
    /// or only track the dirty nodes so that one more call to calculate is required.</param>
    /// <param name="minimumTreeLevelToMemoizeKeccak">Minimum lvl of the tree to memoize the Keccak of a branch node.</param>
    /// <param name="memoizeKeccakEvery">How often (which lvl mod) should Keccaks be memoized.</param>
    /// <param name="memoizeRlp">Whether the RLP of branches should be memoized in memory (but not stored)
    /// to limit the number of lookups for the children and their Keccak recalculation.
    /// This includes invalidating the memoized RLP whenever a path that it caches is marked as dirty.</param>
    public ComputeMerkleBehavior(bool fullMerkle = true,
        int minimumTreeLevelToMemoizeKeccak = DefaultMinimumTreeLevelToMemoizeKeccak,
        int memoizeKeccakEvery = MemoizeKeccakEveryNLevel,
        bool memoizeRlp = true)
    {
        _fullMerkle = fullMerkle;
        _minimumTreeLevelToMemoizeKeccak = minimumTreeLevelToMemoizeKeccak;
        _memoizeKeccakEvery = memoizeKeccakEvery;
        _memoizeRlp = memoizeRlp;

        _meter = new Meter("Paprika.Merkle");
        _trieBuilding = _meter.CreateHistogram<long>("Trie building", "ms",
            "How long it takes to build all the Tries of state");
        _stateRootHashCompute = _meter.CreateHistogram<long>("State root compute", "ms",
            "How long it takes to calculate the root hash");
        _storageRootsHashCompute = _meter.CreateHistogram<long>("Storage roots compute", "ms",
            "How long it takes to calculate storage roots");
        _totalMerkle = _meter.CreateHistogram<long>("Total Merkle", "ms",
            "How long it takes to run Merkle");
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
        var value = Compute(in root, wrapper, TrieType.State, hint);
        return new Keccak(value.Span);
    }

    public Keccak CalculateStorageHash(IReadOnlyWorldState commit, in Keccak account, NibblePath storagePath = default)
    {
        const ComputeHint hint = ComputeHint.DontUseParallel | ComputeHint.SkipCachedInformation;
        var prefixed = new PrefixingCommit(new CommitWrapper(commit));
        prefixed.SetPrefix(account);

        var root = Key.Merkle(storagePath);
        var value = Compute(in root, prefixed, TrieType.Storage, hint);
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

        public ReadOnlySpanOwner<byte> Get(scoped in Key key) => _readOnly.Get(key);

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

    public Keccak BeforeCommit(ICommit commit)
    {
        var total = Stopwatch.StartNew();

        try
        {
            // There are following things to do:
            // 1. Visit State Trie 
            // 2. Visit Storage Tries
            // 3. Calculate Storage Roots
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

            // 1&2
            var workItems = new List<IWorkItem>();

            var (buildTrie, computeRoots) = GetStorageWorkItems(commit);

            workItems.AddRange(GetBuildStateTrieWorkItems(commit));
            workItems.AddRange(buildTrie);

            ScatterGather(commit, workItems, _trieBuilding);

            if (_fullMerkle)
            {
                ScatterGather(commit, computeRoots, _storageRootsHashCompute);

                var sw = Stopwatch.StartNew();

                var root = Key.Merkle(NibblePath.Empty);
                var rootKeccak = Compute(root, commit, TrieType.State, ComputeHint.None);

                _stateRootHashCompute.Record(sw.ElapsedMilliseconds);

                Debug.Assert(rootKeccak.DataType == KeccakOrRlp.Type.Keccak);

                var value = new Keccak(rootKeccak.Span);
                RootHash = value;

                return value;
            }

            return Keccak.Zero;
        }
        finally
        {
            _totalMerkle.Record(total.ElapsedMilliseconds);
        }
    }

    /// <summary>
    /// Runs the work items in parallel then gathers the data and commits to the parent.
    /// </summary>
    /// <param name="commit">The original commit.</param>
    /// <param name="workItems">The work items.</param>
    /// <param name="report"></param>
    private static void ScatterGather(ICommit commit, IEnumerable<IWorkItem> workItems, Histogram<long> report)
    {
        var sw = Stopwatch.StartNew();

        var children = new ConcurrentQueue<IChildCommit>();

        Parallel.ForEach(workItems, commit.GetChild, (workItem, _, child) =>
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

        report.Record(sw.ElapsedMilliseconds);
    }

    /// <summary>
    /// Builds works items responsible for building up the state tree. Work items are split by the leading nibble key.
    /// </summary>
    private static IEnumerable<IWorkItem> GetBuildStateTrieWorkItems(ICommit commit)
    {
        yield return new BuildStateTreeItem(commit, commit.Stats.Keys);
    }

    /// <summary>
    /// Builds works items responsible for building up the storage tries.
    /// </summary>
    private (IEnumerable<IWorkItem> buildTrie, IEnumerable<IWorkItem> computeRoots) GetStorageWorkItems(
        ICommit commit)
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

        return (list.Select(set => new BuildStorageTriesItem(commit, set)).ToArray(),
            list.Select(set => new CalculateStorageTriesRootHashItem(this, set)).ToArray());
    }

    public ReadOnlySpan<byte> InspectBeforeApply(in Key key, ReadOnlySpan<byte> data)
    {
        if (data.IsEmpty)
            return data;

        if (key.Type != DataType.Merkle)
            return data;

        if (Node.Header.Peek(data).NodeType != Node.Type.Branch)
            return data;

        // trim the cached rlp from branches
        return Node.Branch.GetOnlyBranchData(data);
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

    private KeccakOrRlp Compute(in Key key, ICommit commit, TrieType trieType, ComputeHint hint)
    {
        using var owner = commit.Get(key);
        if (owner.IsEmpty)
        {
            // empty tree, return empty
            return Keccak.EmptyTreeHash;
        }

        var leftover = Node.ReadFrom(owner.Span, out var type, out var leaf, out var ext, out var branch);
        switch (type)
        {
            case Node.Type.Leaf:
                return EncodeLeaf(key, commit, leaf, trieType, hint);
            case Node.Type.Extension:
                return EncodeExtension(key, commit, ext, trieType, hint);
            case Node.Type.Branch:
                var useMemoized = !hint.HasFlag(ComputeHint.SkipCachedInformation);
                if (useMemoized && branch.HasKeccak)
                {
                    // return memoized value
                    return branch.Keccak;
                }

                return EncodeBranch(key, commit, branch, trieType, leftover, owner.IsOwnedBy(commit), hint);
            default:
                throw new ArgumentOutOfRangeException();
        }
    }

    private KeccakOrRlp EncodeLeaf(Key key, ICommit commit, scoped in Node.Leaf leaf, TrieType trieType,
        ComputeHint hint)
    {
        var leafPath =
            key.Path.Append(leaf.Path, stackalloc byte[key.Path.MaxByteLength + leaf.Path.MaxByteLength + 1]);

        var leafKey = trieType == TrieType.State
            ? Key.Account(leafPath)
            // the prefix will be added by the prefixing commit
            : Key.Raw(leafPath, DataType.StorageCell, NibblePath.Empty);

        using var leafData = commit.Get(leafKey);

        KeccakOrRlp keccakOrRlp;
        if (trieType == TrieType.State)
        {
            Account.ReadFrom(leafData.Span, out var account);

            if (hint.HasFlag(ComputeHint.ForceStorageRootHashRecalculation))
            {
                var prefixed = new PrefixingCommit(commit);
                prefixed.SetPrefix(leafKey.Path);
                var storageRoot = Compute(Key.Merkle(NibblePath.Empty), prefixed, TrieType.Storage, hint);
                account = new Account(account.Balance, account.Nonce, account.CodeHash, new Keccak(storageRoot.Span));
            }

            Node.Leaf.KeccakOrRlp(leaf.Path, account, out keccakOrRlp);
            return keccakOrRlp;
        }

        Debug.Assert(trieType == TrieType.Storage, "Only accounts now");

        Node.Leaf.KeccakOrRlp(leaf.Path, leafData.Span, out keccakOrRlp);
        return keccakOrRlp;
    }

    private KeccakOrRlp EncodeBranch(Key key, ICommit commit, scoped in Node.Branch branch, TrieType trieType,
        ReadOnlySpan<byte> previousRlp, bool isOwnedByCommit, ComputeHint hint)
    {
        // Parallelize at the root level any trie, state or storage, that have all children set.
        // This heuristic is used to estimate that the tree should be big enough to gain from making this computation
        // parallel but without calculating and storing additional information how big is the tree.
        var runInParallel = !hint.HasFlag(ComputeHint.DontUseParallel) && key.Path.IsEmpty && branch.Children.AllSet;

        var memoize = !hint.HasFlag(ComputeHint.SkipCachedInformation) && ShouldMemoizeBranchRlp(key.Path);
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
                    var value = Compute(Key.Merkle(childPath), commit, trieType, hint);

                    // it's either Keccak or a span. Both are encoded the same ways
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

            // parallel calculation
            Parallel.For((long)0, NibbleSet.NibbleCount, nibble =>
            {
                var childPath = NibblePath.FromKey(stackalloc byte[1] { (byte)(nibble << NibblePath.NibbleShift) }, 0)
                    .SliceTo(1);
                var child = commits[nibble] = commit.GetChild();
                results[nibble] = Compute(Key.Merkle(childPath), child, trieType, hint).Span.ToArray();
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
                commit.SetBranch(key, branch.Children, new Keccak(result.Span), memo.Raw);
            }
            else
            {
                commit.SetBranch(key, branch.Children, memo.Raw);
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

    private KeccakOrRlp EncodeExtension(in Key key, ICommit commit, scoped in Node.Extension ext,
        TrieType trieType, ComputeHint hint)
    {
        Span<byte> span = stackalloc byte[Math.Max(ext.Path.HexEncodedLength, key.Path.MaxByteLength + 1)];

        // retrieve the children keccak-or-rlp
        var branchKeccakOrRlp = Compute(Key.Merkle(key.Path.Append(ext.Path, span)), commit, trieType, hint);

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

        public ReadOnlySpanOwner<byte> Get(scoped in Key key) => _commit.Get(Build(key));

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

            public ReadOnlySpanOwner<byte> Get(scoped in Key key) => _commit.Get(_parent.Build(key));

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
    private static DeleteStatus Delete(in NibblePath path, int at, ICommit commit)
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
                    var status = Delete(path, newAt, commit);

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

                    var newAt = at + 1;

                    var status = Delete(path, newAt, commit);
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
                        // if either has the keccak or has the leftover rlp, clean
                        if (branch.HasKeccak || leftover.IsEmpty == false)
                        {
                            // reset
                            commit.SetBranch(key, branch.Children);
                        }

                        return DeleteStatus.NodeTypePreserved;
                    }

                    Debug.Assert(status == DeleteStatus.LeafDeleted, "leaf deleted");

                    var children = branch.Children.Remove(nibble);

                    // if branch has still more than one child, just update the set
                    if (children.SetCount > 1)
                    {
                        commit.SetBranch(key, children);
                        return DeleteStatus.NodeTypePreserved;
                    }

                    // there's an only child now. The branch should be collapsed
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
                throw new ArgumentOutOfRangeException();
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

    private static void MarkPathDirty(in NibblePath path, ICommit commit)
    {
        Span<byte> span = stackalloc byte[33];

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
                            // update in place, mark in parent as dirty, beside that, do from from the Merkle pov
                            return;
                        }

                        var nibbleA = leaf.Path[diffAt];
                        var nibbleB = leftoverPath[diffAt];

                        // nibbleA, deep copy to write in an unsafe manner
                        var pathA = path.SliceTo(i + diffAt).AppendNibble(nibbleA, span);
                        commit.SetLeaf(Key.Merkle(pathA), leaf.Path.SliceFrom(diffAt + 1));

                        // nibbleB, set the newly set leaf, slice to the next nibble
                        var pathB = path.SliceTo(i + 1 + diffAt);
                        commit.SetLeaf(Key.Merkle(pathB), leftoverPath.SliceFrom(diffAt + 1));

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
                                commit.SetLeaf(Key.Merkle(path.SliceTo(i + 1)), path.SliceFrom(i + 1));
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

                                commit.SetLeaf(Key.Merkle(path.SliceTo(i + 1)), path.SliceFrom(i + 1));

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
                            commit.SetLeaf(Key.Merkle(path.SliceTo(splitAt + 1)), path.SliceFrom(splitAt + 1));

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
                        commit.SetLeaf(Key.Merkle(leafPath), path.SliceFrom(leafPath.Length));

                        // Important! Make it the last as it's updating the existing key
                        commit.SetExtension(key, extPath);

                        return;
                    }
                case Node.Type.Branch:
                    {
                        var nibble = path[i];
                        Span<byte> rlp = default;
                        byte[]? array = default;

                        if (leftover.Length == RlpMemo.Size)
                        {
                            if (owner.IsOwnedBy(commit))
                            {
                                rlp = MakeRlpWritable(leftover);
                            }
                            else
                            {
                                array = ArrayPool<byte>.Shared.Rent(RlpMemo.Size);
                                rlp = array.AsSpan(0, RlpMemo.Size);
                                leftover.CopyTo(rlp);
                            }

                            var memo = new RlpMemo(rlp);
                            memo.Clear(nibble);
                        }

                        if (rlp.IsEmpty)
                        {
                            // update branch as is, as there's no rlp
                            commit.SetBranch(key, branch.Children.Set(nibble));
                        }
                        else
                        {
                            commit.SetBranch(key, branch.Children.Set(nibble), rlp);
                        }

                        if (array != null)
                        {
                            ArrayPool<byte>.Shared.Return(array);
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
        void DoWork(IChildCommit commit);
    }

    /// <summary>
    /// Builds a part of State Trie, invalidating paths and marking them as dirty whenever needed.
    /// </summary>
    private sealed class BuildStateTreeItem : IWorkItem
    {
        private readonly ICommit _parent;
        private readonly HashSet<Keccak> _toTouch;

        private IChildCommit? _commit;

        public BuildStateTreeItem(ICommit parent, IEnumerable<Keccak> toTouch)
        {
            _parent = parent;
            _toTouch = new HashSet<Keccak>(toTouch);
            _commit = null;
        }

        public void DoWork(IChildCommit commit)
        {
            _commit = commit;
            _parent.Visit(OnState, TrieType.State);

            // dirty the leftovers
            foreach (var key in _toTouch)
            {
                MarkPathDirty(NibblePath.FromKey(key), _commit!);
            }
        }

        private void OnState(in Key key, ReadOnlySpan<byte> value)
        {
            Debug.Assert(key.Type == DataType.Account);

            if (value.IsEmpty)
            {
                Delete(in key.Path, 0, _commit!);
            }
            else
            {
                MarkPathDirty(in key.Path, _commit!);
            }

            // mark as touched already
            _toTouch.Remove(key.Path.UnsafeAsKeccak);
        }
    }

    private sealed class BuildStorageTriesItem : IWorkItem
    {
        private readonly ICommit _parent;
        private readonly HashSet<Keccak> _accountsToProcess;
        private PrefixingCommit? _commit;

        public BuildStorageTriesItem(ICommit parent, HashSet<Keccak> accountsToProcess)
        {
            _parent = parent;
            _accountsToProcess = accountsToProcess;
            _commit = null;
        }

        public void DoWork(IChildCommit commit)
        {
            _commit = new PrefixingCommit(commit);
            _parent.Visit(OnStorage, TrieType.Storage);
        }

        private void OnStorage(in Key key, ReadOnlySpan<byte> value)
        {
            Debug.Assert(key.Type == DataType.StorageCell);

            var keccak = key.Path.UnsafeAsKeccak;
            if (_accountsToProcess.Contains(keccak) == false)
            {
                return;
            }

            _commit!.SetPrefix(keccak);

            if (value.IsEmpty)
            {
                Delete(in key.StoragePath, 0, _commit);
            }
            else
            {
                MarkPathDirty(in key.StoragePath, _commit);
            }
        }
    }

    private sealed class CalculateStorageTriesRootHashItem : IWorkItem
    {
        private readonly ComputeMerkleBehavior _behavior;
        private readonly HashSet<Keccak> _accounts;

        public CalculateStorageTriesRootHashItem(ComputeMerkleBehavior behavior, HashSet<Keccak> accounts)
        {
            _behavior = behavior;
            _accounts = accounts;
        }

        public void DoWork(IChildCommit commit)
        {
            // If there are more accounts to go through than 1, then it means that they are grouped to make the compute parallel.
            // Don't parallelize this work as it would be counter-productive to have parallel over parallel.
            // If there's a one account only, use the default.
            var hint = _accounts.Count > 1 ? ComputeHint.DontUseParallel : ComputeHint.None;

            Span<byte> accountSpan = stackalloc byte[Account.MaxByteCount];
            var prefixed = new PrefixingCommit(commit);

            foreach (var accountAddress in _accounts)
            {
                prefixed.SetPrefix(accountAddress);

                // compute new storage root hash
                var keccakOrRlp = _behavior.Compute(Key.Merkle(NibblePath.Empty), prefixed, TrieType.Storage, hint);

                // read the existing account
                var key = Key.Account(accountAddress);
                using var accountOwner = commit.Get(key);

                Debug.Assert(accountOwner.IsEmpty == false, "The account should exist");

                Account.ReadFrom(accountOwner.Span, out var account);

                // update it
                account = account.WithChangedStorageRoot(new Keccak(keccakOrRlp.Span));

                // set it
                commit.Set(key, account.WriteTo(accountSpan));
            }
        }
    }

    public void Dispose() => _meter.Dispose();
}