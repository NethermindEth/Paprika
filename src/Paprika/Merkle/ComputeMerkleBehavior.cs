using System.Buffers;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using Paprika.Chain;
using Paprika.Crypto;
using Paprika.Data;
using Paprika.RLP;

namespace Paprika.Merkle;

/// <summary>
/// This is the Merkle component that works with the Merkle-agnostic part of Paprika.
///
/// It splits the Merkle part into two areas:
/// 1. building Merkle tree, that reads the data written in a given <see cref="ICommit"/> and applies it to create Merkle
///  construct.
/// 2. calls the computation of the Merkle RootHash when needed.
/// </summary>
public class ComputeMerkleBehavior : IPreCommitBehavior
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

    public ComputeMerkleBehavior(bool fullMerkle = false,
        int minimumTreeLevelToMemoizeKeccak = DefaultMinimumTreeLevelToMemoizeKeccak,
        int memoizeKeccakEvery = MemoizeKeccakEveryNLevel)
    {
        _fullMerkle = fullMerkle;
        _minimumTreeLevelToMemoizeKeccak = minimumTreeLevelToMemoizeKeccak;
        _memoizeKeccakEvery = memoizeKeccakEvery;
    }

    public void BeforeCommit(ICommit commit)
    {
        // run the visitor on the commit
        commit.Visit(OnKey, TrieType.State);

        if (_fullMerkle)
        {
            var root = Key.Merkle(NibblePath.Empty);
            var keccakOrRlp = Compute(root, commit, TrieType.State);

            Debug.Assert(keccakOrRlp.DataType == KeccakOrRlp.Type.Keccak);

            RootHash = new Keccak(keccakOrRlp.Span);
        }
    }

    public Keccak RootHash { get; private set; }

    [SkipLocalsInit]
    private KeccakOrRlp Compute(in Key key, ICommit commit, TrieType trieType)
    {
        using var owner = commit.Get(key);
        if (owner.IsEmpty)
        {
            // empty tree, return empty
            return Keccak.EmptyTreeHash;
        }

        Node.ReadFrom(owner.Span, out var type, out var leaf, out var ext, out var branch);
        switch (type)
        {
            case Node.Type.Leaf:
                return EncodeLeaf(key, commit, leaf, trieType);
            case Node.Type.Extension:
                return EncodeExtension(key, commit, ext, trieType);
            case Node.Type.Branch:
                if (branch.HasKeccak)
                {
                    // return memoized value
                    return branch.Keccak;
                }

                return EncodeBranch(key, commit, branch, trieType);
            default:
                throw new ArgumentOutOfRangeException();
        }
    }

    private static KeccakOrRlp EncodeLeaf(Key key, ICommit commit, scoped in Node.Leaf leaf, TrieType trieType)
    {
        var leafPath =
            key.Path.Append(leaf.Path, stackalloc byte[key.Path.MaxByteLength + leaf.Path.MaxByteLength + 1]);

        Debug.Assert(trieType == TrieType.State, "Only accounts now");

        using var leafData = commit.Get(Key.Account(leafPath));

        Account.ReadFrom(leafData.Span, out var account);
        Node.Leaf.KeccakOrRlp(leaf.Path, account, out var keccakOrRlp);

        return keccakOrRlp;
    }

    private KeccakOrRlp EncodeBranch(Key key, ICommit commit, scoped in Node.Branch branch, TrieType trieType)
    {
        var bytes = ArrayPool<byte>.Shared.Rent(MaxBufferNeeded);

        // leave for length preamble
        const int initialShift = Rlp.MaxLengthOfLength + 1;
        var stream = new RlpStream(bytes)
        {
            Position = initialShift
        };

        const int additionalBytesForNibbleAppending = 1;
        Span<byte> childSpan = stackalloc byte[key.Path.MaxByteLength + additionalBytesForNibbleAppending];

        for (byte i = 0; i < NibbleSet.NibbleCount; i++)
        {
            if (branch.Children[i])
            {
                var childPath = key.Path.AppendNibble(i, childSpan);
                var value = Compute(Key.Merkle(childPath), commit, trieType);

                // it's either Keccak or a span. Both are encoded the same ways
                stream.Encode(value.Span);
            }
            else
            {
                stream.EncodeEmptyArray();
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

        if (result.DataType == KeccakOrRlp.Type.Keccak && ShouldMemoizeBranchKeccak(key.Path))
        {
            // Memoize only if Keccak and falls into the criteria.
            // Storing RLP for an embedded node is useless as it can be easily re-calculated.
            commit.SetBranch(key, branch.Children, new Keccak(result.Span));
        }

        return result;
    }

    private bool ShouldMemoizeBranchKeccak(in NibblePath branchPath)
    {
        var level = branchPath.Length - _minimumTreeLevelToMemoizeKeccak;

        // memoize only if the branch is deeper than _minimumTreeLevelToMemoizeKeccak and every _memoizeKeccakEvery
        return level >= 0 && level % _memoizeKeccakEvery == 0;
    }

    private KeccakOrRlp EncodeExtension(in Key key, ICommit commit, scoped in Node.Extension ext,
        TrieType trieType)
    {
        Span<byte> span = stackalloc byte[Math.Max(ext.Path.HexEncodedLength, key.Path.MaxByteLength + 1)];

        // retrieve the children keccak-or-rlp
        var branchKeccakOrRlp = Compute(Key.Merkle(key.Path.Append(ext.Path, span)), commit, trieType);

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

    private static void OnKey(in Key key, ReadOnlySpan<byte> value, ICommit commit)
    {
        if (value.IsEmpty)
        {
            Delete(in key.Path, 0, commit);
        }
        else
        {
            MarkPathDirty(in key.Path, commit);
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
        Node.ReadFrom(owner.Span, out var type, out var leaf, out var ext, out var branch);
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
                        if (branch.HasKeccak)
                        {
                            // reset keccak
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

        for (int i = 0; i < path.Length; i++)
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
            Node.ReadFrom(owner.Span, out var type, out var leaf, out var ext, out var branch);
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

                        if (diffAt > 0)
                        {
                            // diff is not on the 0th position, so it will be a branch but preceded with an extension
                            commit.SetExtension(key, leftoverPath.SliceTo(diffAt));
                        }

                        var nibbleA = leaf.Path[diffAt];
                        var nibbleB = leftoverPath[diffAt];

                        // create branch, truncate both leaves, add them at the end
                        var branchKey = Key.Merkle(path.SliceTo(i + diffAt));
                        commit.SetBranch(branchKey, new NibbleSet(nibbleA, nibbleB));

                        // nibbleA, deep copy to write in an unsafe manner
                        var pathA = path.SliceTo(i + diffAt).AppendNibble(nibbleA, span);
                        commit.SetLeaf(Key.Merkle(pathA), leaf.Path.SliceFrom(diffAt + 1));

                        // nibbleB, set the newly set leaf, slice to the next nibble
                        var pathB = path.SliceTo(i + 1 + diffAt);
                        commit.SetLeaf(Key.Merkle(pathB), leftoverPath.SliceFrom(diffAt + 1));

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

                                commit.SetBranch(key, new NibbleSet(ext0Th, leftoverPath[0]));

                                commit.SetExtension(Key.Merkle(key.Path.AppendNibble(ext0Th, span)),
                                    ext.Path.SliceFrom(1));

                                commit.SetLeaf(Key.Merkle(path.SliceTo(i + 1)), path.SliceFrom(i + 1));
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

                            commit.SetExtension(key, ext.Path.SliceTo(lastNibblePos));

                            var splitAt = i + ext.Path.Length - 1;
                            var set = new NibbleSet(path[splitAt], ext.Path[lastNibblePos]);

                            commit.SetBranch(Key.Merkle(path.SliceTo(splitAt)), set);
                            commit.SetLeaf(Key.Merkle(path.SliceTo(splitAt + 1)), path.SliceFrom(splitAt + 1));

                            return;
                        }

                        // the diff is not at the 0th nibble, it's not a full match as well
                        // this means that E0->B0 will turn into E1->B1->E2->B0
                        //                                             ->L0
                        var extPath = ext.Path.SliceTo(diffAt);
                        commit.SetExtension(key, extPath);

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
                            var e2Path = ext.Path.SliceFrom(extension2.Length);
                            commit.SetExtension(Key.Merkle(extension2), e2Path);
                        }

                        // L0
                        var leafPath = branch1.AppendNibble(addedNibble, span);
                        commit.SetLeaf(Key.Merkle(leafPath), path.SliceFrom(leafPath.Length));

                        return;
                    }
                case Node.Type.Branch:
                    {
                        var nibble = path[i];
                        if (branch.HasKeccak)
                        {
                            // branch has keccak, this means it was not written yet, needs to be dirtied
                            commit.SetBranch(key, branch.Children.Set(nibble));
                        }
                        else
                        {
                            if (branch.Children[nibble] && !branch.HasKeccak)
                            {
                                // if child is set and there's no keccak for the branch, everything set as needed
                                continue;
                            }

                            commit.SetBranch(key, branch.Children.Set(nibble));
                        }
                    }
                    break;
                default:
                    throw new ArgumentOutOfRangeException();
            }
        }
    }
}