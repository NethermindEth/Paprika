using System.Diagnostics;
using Paprika.Chain;
using Paprika.Crypto;
using Paprika.Data;
using Paprika.RLP;

namespace Paprika.Merkle;

/// <summary>
/// Extensions to the commit used to set values of the 
/// </summary>
public static class CommitExtensions
{
    public static void SetLeaf(this ICommit commit, in Key key, in NibblePath leafPath)
    {
        var leaf = new Node.Leaf(leafPath);
        commit.Set(key, leaf.WriteTo(stackalloc byte[leaf.MaxByteLength]));
    }

    public static void SetBranch(this ICommit commit, in Key key, NibbleSet.Readonly children)
    {
        var branch = new Node.Branch(children);
        commit.Set(key, branch.WriteTo(stackalloc byte[branch.MaxByteLength]));
    }

    public static void SetBranch(this ICommit commit, in Key key, NibbleSet.Readonly children, KeccakOrRlp keccak)
    {
        Debug.Assert(keccak.DataType == KeccakOrRlp.Type.Keccak);
        var actual = new Keccak(keccak.Span);

        var branch = new Node.Branch(children, actual);
        commit.Set(key, branch.WriteTo(stackalloc byte[branch.MaxByteLength]));
    }

    public static void SetExtension(this ICommit commit, in Key key, in NibblePath path)
    {
        var extension = new Node.Extension(path);
        commit.Set(key, extension.WriteTo(stackalloc byte[extension.MaxByteLength]));
    }

    public static void DeleteKey(this ICommit commit, in Key key) => commit.Set(key, ReadOnlySpan<byte>.Empty);
}