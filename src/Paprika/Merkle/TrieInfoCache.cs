using System.Collections.Specialized;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Paprika.Data;

namespace Paprika.Merkle;

/// <summary>
/// A struct used to memoize top levels of a Trie that has a relatively big number of paths to be dirtied with
/// <see cref="ComputeMerkleBehavior.MarkPathDirty"/>.
///
/// It saves a lot of queries and checks.   
/// </summary>
[StructLayout(LayoutKind.Explicit, Size = L0Size + L1Size + L2Size)]
struct TrieStructureCache
{
    public const int MaxMemoizedLevel = 2;
    private const byte NibbleCount = 16;
    private const byte ByteSize = 8;
    private const int L0Size = 1;
    private const int L1Size = NibbleCount / ByteSize;
    private const int L2Size = NibbleCount * NibbleCount / ByteSize;

    [FieldOffset(0)] private byte _l0;
    [FieldOffset(L0Size)] private byte _l1;
    [FieldOffset(L0Size + L1Size)] private byte _l2;

    /// <summary>
    /// Gets the start nibble of the path that is not dirtied and memoized yet and requires visiting.
    /// </summary>
    /// <param name="path"></param>
    /// <returns></returns>
    public int GetCachedStart(in NibblePath path)
    {
        return 0;
    }

    public void MarkAsDirtyBranchAt(in NibblePath path, int at)
    {
        switch (at)
        {
            case 0:
                _l0 = 1;
                break;
            case 1:
                SetBit(path.GetAt(0), ref _l1);
                break;
            case 2:
                SetBit(path.GetAt(0), ref _l2);
                break;
        }

        return;

        static void SetBit(int value, ref byte b)
        {
            var (@byte, bit) = Split(value);
            ref var @ref = ref Unsafe.Add(ref b, @byte);

            @ref = (byte)((1 << bit) | @ref);
        }

        static (int @byte, int bit) Split(int value) => (value >> 3, value & 7);
    }
}