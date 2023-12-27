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
    private readonly NibbleSet.Readonly _leafs;
    private readonly ReadOnlySpan<byte> _paths;

    private EmbeddedLeafs(NibbleSet.Readonly leafs, ReadOnlySpan<byte> paths)
    {
        _leafs = leafs;
        _paths = paths;
    }

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

        var raw = leafPath.RawSpan;
        var length = raw.Length;

        var beforeCount = _leafs.SetCountToNibble(first);
        var slice = beforeCount * length;

        var encodedPath = _paths.Slice(slice, length);

        leaf = new Node.Leaf(leafPath.ReplaceRaw(encodedPath));
        return true;
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
        leftover = _paths.WriteToWithLeftover(leftover);
        return destination.Slice(0, destination.Length - leftover.Length);
    }

    public Span<byte> WriteTo(Span<byte> output)
    {
        var leftover = WriteToWithLeftover(output);
        return output.Slice(0, output.Length - leftover.Length);
    }
}