using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Paprika.Data;
using Paprika.Store;

namespace Paprika.Merkle;

/// <summary>
/// A struct used to memoize top levels of a Trie that has a relatively big number of paths to be dirtied with
/// <see cref="ComputeMerkleBehavior.MarkPathDirty"/>.
///
/// It saves a lot of queries and checks.   
/// </summary>
readonly ref struct TrieStructureCache
{
    private readonly Span<NibbleSet> _nibbles;

    private const byte NibbleCount = 16;

    public TrieStructureCache(in Page page)
    {
        unsafe
        {
            _nibbles = page.Raw == UIntPtr.Zero
                ? Span<NibbleSet>.Empty
                : new Span<NibbleSet>(page.Raw.ToPointer(), Page.PageSize / Unsafe.SizeOf<NibbleSet>());
        }
    }

    public TrieStructureCache(byte[] buffer)
    {
        _nibbles = MemoryMarshal.Cast<byte, NibbleSet>(buffer.AsSpan());
    }

    /// <summary>
    /// Gets the start nibble of the path that is not dirtied and memoized yet and requires visiting.
    /// </summary>
    /// <param name="path"></param>
    /// <returns></returns>
    public int GetCachedStart(in NibblePath path)
    {
        var at = 0;
        var start = 0;
        var count = 1;

        while (start + count <= _nibbles.Length)
        {
            var current = _nibbles.Slice(start, count);
            var value = BuildValue(path, at);

            var (set, nibble) = Math.DivRem(value, NibbleCount);

            if (current[set][(byte)nibble] == false)
            {
                return at;
            }

            at++;
            start += count;
            count *= NibbleCount;
        }

        return at;
    }

    public void MarkAsVisitedBranchAt(in NibblePath path, int at)
    {
        var i = 0;
        var start = 0;
        var count = 1;

        while (start + count <= _nibbles.Length)
        {
            if (at == i)
            {
                var current = _nibbles.Slice(start, count);
                var value = BuildValue(path, at);
                var (set, nibble) = Math.DivRem(value, NibbleCount);
                current[set][(byte)nibble] = true;

                return;
            }

            i++;
            start += count;
            count *= NibbleCount;
        }
    }

    public void InvalidateFrom(in NibblePath path, int i)
    {
        var at = 0;
        var start = 0;
        var count = 1;

        while (start + count <= _nibbles.Length)
        {
            if (i >= at)
            {
                var current = _nibbles.Slice(start, count);
                var value = BuildValue(path, at);

                var (set, nibble) = Math.DivRem(value, NibbleCount);

                current[set][(byte)nibble] = false;
            }

            at++;
            start += count;
            count *= NibbleCount;
        }
    }

    private static int BuildValue(in NibblePath path, int at)
    {
        return at switch
        {
            0 => path.GetAt(0),
            1 => path.GetAt(0) | (path.GetAt(1) << NibblePath.NibbleShift),
            2 => path.GetAt(0) | (path.GetAt(1) << NibblePath.NibbleShift) | (path.GetAt(2) << (NibblePath.NibbleShift * 2)),
            _ => -1
        };
    }
}