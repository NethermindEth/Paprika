using System.Diagnostics;
using System.Runtime.CompilerServices;

using Paprika.Chain;
using Paprika.Crypto;
using Paprika.Data;
using Paprika.RLP;

using static Paprika.Merkle.Node;

namespace Paprika.Merkle;

/// <summary>
/// Extensions to the commit used to set values of the 
/// </summary>
public static class CommitExtensions
{
    [SkipLocalsInit]
    public static void SetLeaf(this ICommit commit, in Key key, in NibblePath leafPath,
        EntryType type = EntryType.Persistent)
    {
        if (leafPath.Length == 0)
        {
            // Last level leafs are omitted.
            return;
        }

        var leaf = new Leaf(leafPath);
        commit.Set(key, leaf.WriteTo(stackalloc byte[Leaf.MaxByteLength]), type);
    }

    [SkipLocalsInit]
    public static void SetBranch(this ICommit commit, in Key key, NibbleSet.Readonly children,
        EntryType type = EntryType.Persistent)
    {
        var branch = new Branch(children);
        commit.Set(key, branch.WriteTo(stackalloc byte[Branch.MaxByteLength]), RlpMemo.Empty, type);
    }

    [SkipLocalsInit]
    public static void SetBranch(this ICommit commit, in Key key, NibbleSet.Readonly children, ReadOnlySpan<byte> rlp,
        EntryType type = EntryType.Persistent)
    {
        var branch = new Branch(children);
        commit.Set(key, branch.WriteTo(stackalloc byte[Branch.MaxByteLength]), rlp, type);
    }

    [SkipLocalsInit]
    public static void SetExtension(this ICommit commit, in Key key, in NibblePath path, EntryType type = EntryType.Persistent)
    {
        var extension = new Extension(path);
        commit.Set(key, extension.WriteTo(stackalloc byte[Extension.MaxByteLength]), type);
    }

    public static void DeleteKey(this ICommit commit, in Key key) => commit.Set(key, ReadOnlySpan<byte>.Empty);
}
