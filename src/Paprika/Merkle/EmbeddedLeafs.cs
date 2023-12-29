using System.Diagnostics;
using Paprika.Crypto;
using Paprika.Data;

namespace Paprika.Merkle;

/// <summary>
/// The structure represents <see cref="Node.Branch"/> leafs densely packed.
/// </summary>
/// <remarks>
/// The reason to have them embedded within the branch is to limit the read amplification as well as reduce the db size. 
/// </remarks>
public readonly ref struct EmbeddedLeafs
{
    public const int MaxWorksetNeeded = 16 * Keccak.Size;

    public static int PathSpanSize(int count) => Keccak.Size * count;

    private readonly NibbleSet.Readonly _leafs;
    private readonly ReadOnlySpan<byte> _paths;

    public EmbeddedLeafs(scoped in NibblePath a, Span<byte> destination)
    {
        _leafs = new NibbleSet(a.FirstNibble);

        a.RawSpan.CopyTo(destination);

        _paths = destination.Slice(0, a.RawSpan.Length);
    }

    public EmbeddedLeafs(scoped in NibblePath a, scoped in NibblePath b, Span<byte> destination)
    {
        _leafs = new NibbleSet(a.FirstNibble, b.FirstNibble);

        var length = a.RawSpan.Length;
        Debug.Assert(length == b.RawSpan.Length);

        if (a.FirstNibble < b.FirstNibble)
        {
            a.RawSpan.CopyTo(destination);
            b.RawSpan.CopyTo(destination.Slice(length));
        }
        else
        {
            // opposite order, write b first
            b.RawSpan.CopyTo(destination);
            a.RawSpan.CopyTo(destination.Slice(length));
        }

        _paths = destination.Slice(0, length * 2);
    }

    private EmbeddedLeafs(NibbleSet.Readonly leafs, ReadOnlySpan<byte> paths)
    {
        _leafs = leafs;
        _paths = paths;
    }

    public int MaxByteSize => NibbleSet.MaxByteSize + _paths.MaxByteLength();

    public bool IsEmpty => _leafs.SetCount == 0;

    public int Count => _leafs.SetCount;

    /// <summary>
    /// Tries to get the leaf with the same first nibble as the <paramref name="leafPath"/>.
    /// </summary>
    public bool TryGetLeafWithSameNibble(in NibblePath leafPath, out Node.Leaf leaf)
    {
        var first = leafPath.FirstNibble;

        if (_leafs[first] == false)
        {
            leaf = default;
            return false;
        }

        var length = GetRawLength(leafPath);

        var beforeCount = _leafs.SetCountToNibble(first);
        var slice = beforeCount * length;

        var encodedPath = _paths.Slice(slice, length);

        leaf = new Node.Leaf(leafPath.ReplaceRaw(encodedPath));
        return true;
    }

    private static int GetRawLength(NibblePath leafPath) => leafPath.RawSpan.Length;

    public EmbeddedLeafs Add(scoped in NibblePath path, Span<byte> destination)
    {
        var nibble = path.FirstNibble;
        Debug.Assert(_leafs[nibble] == false, "The child must not be set");

        var before = _leafs.SetCountToNibble(nibble);
        var length = GetRawLength(path);

        // copy nibbles before
        _paths.Slice(0, before * length).CopyTo(destination);

        // copy this leaf
        path.RawSpan.CopyTo(destination.Slice(before * length));

        // copy after
        _paths.Slice(before * length).CopyTo(destination.Slice((before + 1) * length));

        var leafs = _leafs.Set(nibble);
        return new EmbeddedLeafs(leafs, destination.Slice(0, leafs.SetCount * length));
    }

    public EmbeddedLeafs Remove(in NibblePath path, Span<byte> destination)
    {
        var nibble = path.FirstNibble;
        Debug.Assert(_leafs[nibble], "The child must be set");

        var before = _leafs.SetCountToNibble(nibble);
        var length = GetRawLength(path);

        // copy nibbles before
        _paths.Slice(0, before * length).CopyTo(destination);

        // omit the leaf

        // copy after
        _paths.Slice((before + 1) * length).CopyTo(destination.Slice(before * length));

        var leafs = _leafs.Remove(nibble);
        return new EmbeddedLeafs(leafs, destination.Slice(0, leafs.SetCount * length));
    }

    public static ReadOnlySpan<byte> ReadFrom(ReadOnlySpan<byte> source, out EmbeddedLeafs embedded)
    {
        var leftover = NibbleSet.Readonly.ReadFrom(source, out var leafs);
        leftover = leftover.ReadFrom(out var paths);
        embedded = new EmbeddedLeafs(leafs, paths);
        return leftover;
    }

    public Span<byte> WriteToWithLeftover(Span<byte> destination)
    {
        var leftover = _leafs.WriteToWithLeftover(destination);
        return _paths.WriteToWithLeftover(leftover);
    }

    public NibblePath GetSingleLeaf(in NibblePath otherPath)
    {
        Debug.Assert(_leafs.SetCount == 1, "There must be only one child");
        return otherPath.ReplaceRaw(_paths);
    }
}