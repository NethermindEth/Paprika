using System.Buffers;
using System.Collections;
using System.Numerics;
using System.Runtime.InteropServices.ComTypes;

namespace Paprika.Utils;

/**
 * The xor filter, a new algorithm that can replace a Bloom filter.
 *
 * It needs 1.23 log(1/fpp) bits per key. It is related to the BDZ algorithm [1]
 * (a minimal perfect hash function algorithm).
 *
 * [1] paper: Simple and Space-Efficient Minimal Perfect Hash Functions -
 * http://cmph.sourceforge.net/papers/wads07.pdf
 */
public class Xor8
{
    private const int BitsPerFingerprint = 8;
    private const int Hashes = 3;
    private const int Offset = 32;
    private const int FactorTimes100 = 123;

    private readonly int _blockLength;
    private readonly ulong _seed;
    private readonly byte[] _fingerprints;

    private static int GetArrayLength(int size) => (int)(Offset + (long)FactorTimes100 * size / 100);

    public Xor8(IReadOnlyCollection<int> keys) : this(new ReadonlyUlong(keys))
    {
    }

    public Xor8(IReadOnlyCollection<ulong> keys)
    {
        // TODO: remove all array allocations, use ArrayPool<ulong> more and/or buffer pool, potentially combine chunks of memory together

        
        var size = keys.Count;
        var arrayLength = GetArrayLength(size);

        _blockLength = arrayLength / Hashes;

        var reverseOrder = ArrayPool<ulong>.Shared.Rent(size);
        var reverseH = ArrayPool<byte>.Shared.Rent(size);
        var t2Count = ArrayPool<byte>.Shared.Rent(arrayLength);
        var t2 = ArrayPool<ulong>.Shared.Rent(arrayLength);

        int reverseOrderPos;
        ulong seed;

    MainLoop:
        do
        {
            seed = Hash.RandomSeed();
            reverseOrderPos = 0;
            
            Array.Clear(t2Count);
            Array.Clear(t2);
            
            foreach (var k in keys)
            {
                for (var hi = 0; hi < Hashes; hi++)
                {
                    var h = GetHash(k, seed, hi);
                    t2[h] ^= k;
                    if (t2Count[h] > 120)
                    {
                        // probably something wrong with the hash function
                        goto MainLoop;
                    }

                    t2Count[h]++;
                }
            }

            int[][] alone = new int[Hashes][];
            for (var i = 0; i < Hashes; i++)
            {
                alone[i] = new int[_blockLength];
            }

            // ReSharper disable once StackAllocInsideLoop
#pragma warning disable CA2014
            Span<int> alonePos = stackalloc int[Hashes];
#pragma warning restore CA2014

            for (var nextAlone = 0; nextAlone < Hashes; nextAlone++)
            {
                for (var i = 0; i < _blockLength; i++)
                {
                    if (t2Count[nextAlone * _blockLength + i] == 1)
                    {
                        alone[nextAlone][alonePos[nextAlone]++] = nextAlone * _blockLength + i;
                    }
                }
            }

            var found = -1;
            while (true)
            {
                var i = -1;
                for (var hi = 0; hi < Hashes; hi++)
                {
                    if (alonePos[hi] > 0)
                    {
                        i = alone[hi][--alonePos[hi]];
                        found = hi;
                        break;
                    }
                }

                if (i == -1)
                {
                    // no entry found
                    break;
                }

                if (t2Count[i] <= 0)
                {
                    continue;
                }

                var k = t2[i];
                if (t2Count[i] != 1)
                {
                    throw new Exception("Assertion error");
                }

                --t2Count[i];
                for (var hi = 0; hi < Hashes; hi++)
                {
                    if (hi != found)
                    {
                        var h = GetHash(k, seed, hi);
                        int newCount = --t2Count[h];
                        if (newCount == 1)
                        {
                            alone[hi][alonePos[hi]++] = h;
                        }

                        t2[h] ^= k;
                    }
                }

                reverseOrder[reverseOrderPos] = k;
                reverseH[reverseOrderPos] = (byte)found;
                reverseOrderPos++;
            }
        } while (reverseOrderPos != size);

        _seed = seed;

        var fp = new byte[arrayLength];
        for (var i = reverseOrderPos - 1; i >= 0; i--)
        {
            var k = reverseOrder[i];
            int found = reverseH[i];
            var change = -1;
            var hash = Hash.Hash64(k, seed);
            var xor = Fingerprint(hash);
            for (var hi = 0; hi < Hashes; hi++)
            {
                var h = GetHash(k, seed, hi);
                if (found == hi)
                {
                    change = h;
                }
                else
                {
                    xor ^= fp[h];
                }
            }

            fp[change] = (byte)xor;
        }

        _fingerprints = new byte[arrayLength];
        fp.CopyTo(_fingerprints, 0);

        ArrayPool<ulong>.Shared.Return(reverseOrder);
        ArrayPool<byte>.Shared.Return(reverseH);
        ArrayPool<byte>.Shared.Return(t2Count);
        ArrayPool<ulong>.Shared.Return(t2);
    }

    public bool MayContain(ulong key)
    {
        var hash = Hash.Hash64(key, _seed);
        var f = Fingerprint(hash);
        var r0 = (uint)hash;
        var r1 = (uint)BitOperations.RotateLeft(hash, 21);
        var r2 = (uint)BitOperations.RotateLeft(hash, 42);
        var h0 = Hash.Reduce(r0, _blockLength);
        var h1 = Hash.Reduce(r1, _blockLength) + _blockLength;
        var h2 = Hash.Reduce(r2, _blockLength) + 2 * _blockLength;
        f ^= _fingerprints[h0] ^ _fingerprints[h1] ^ _fingerprints[h2];
        return (f & 0xff) == 0;
    }

    private int GetHash(ulong key, ulong seed, int index)
    {
        var r = BitOperations.RotateLeft(Hash.Hash64(key, seed), 21 * index);
        r = Hash.Reduce((uint)r, _blockLength);
        r = r + (ulong)(index * _blockLength);
        return (int)r;
    }

    private static int Fingerprint(ulong hash) => (int)(hash & ((1 << BitsPerFingerprint) - 1));

    private static class Hash
    {
        public static ulong Hash64(ulong x, ulong seed)
        {
            x += seed;
            x = (x ^ (x >>> 33)) * 0xff51afd7ed558ccdL;
            x = (x ^ (x >>> 33)) * 0xc4ceb9fe1a85ec53L;
            x = x ^ (x >>> 33);
            return x;
        }

        public static ulong RandomSeed() => unchecked((ulong)Random.Shared.NextInt64());

        /**
         * Shrink the hash to a value 0..n. Kind of like modulo, but using
         * multiplication and shift, which are faster to compute.
         *
         * @param hash the hash
         * @param n the maximum of the result
         * @return the reduced value
         */
        public static uint Reduce(uint hash, int n)
        {
            // http://lemire.me/blog/2016/06/27/a-fast-alternative-to-the-modulo-reduction/
            return (uint)(((hash & 0xffffffffL) * (n & 0xffffffffL)) >>> 32);
        }
    }
}

file class ReadonlyUlong : IReadOnlyCollection<ulong>
{
    private readonly IReadOnlyCollection<int> _ints;

    public ReadonlyUlong(IReadOnlyCollection<int> ints)
    {
        _ints = ints;
    }

    public IEnumerator<ulong> GetEnumerator()
    {
        foreach (var i in _ints)
        {
            yield return unchecked((ulong)i);
        }
    }

    IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();

    public int Count => _ints.Count;
}