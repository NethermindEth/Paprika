using System.Runtime.CompilerServices;

namespace Tree;

public class Nibbles
{
    public const int SamePaths = -1;

    public static int FindByteDifference(in ReadOnlySpan<byte> path1, in ReadOnlySpan<byte> path2)
    {
        ref var p1 = ref Unsafe.AsRef(in path1[0]);
        ref var p2 = ref Unsafe.AsRef(in path2[0]);
        return FindByteDifference(p1, p2, path1.Length);
    }
    
    public static int FindByteDifference(in byte path1, in byte path2, int length)
    {
        const int longJump = sizeof(ulong);
        
        ref var p1 = ref Unsafe.AsRef(path1);
        ref var p2 = ref Unsafe.AsRef(path2);

        var position = 0;
        while (position + longJump <= length)
        {
            var u1 = Unsafe.ReadUnaligned<ulong>(ref p1);
            var u2 = Unsafe.ReadUnaligned<ulong>(ref p2);
            
            if (u1 != u2)
            {
                break;
            }

            position += longJump;

            p1 = ref Unsafe.Add(ref p1, longJump);
            p2 = ref Unsafe.Add(ref p2, longJump);
        }

        if (position == length)
        {
            return length;
        }

        // TODO: optimize with Lzcnt.X64, Lzcnt and loop unroll for all different cases
        while (position < length)
        {
            if (p1 != p2)
            {
                return position;
            }
            
            position += 1;
            p1 = ref Unsafe.Add(ref p1, 1);
            p2 = ref Unsafe.Add(ref p2, 1);
        }

        return length;
    }
}