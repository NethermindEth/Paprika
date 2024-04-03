using System.Diagnostics;
using System.Runtime.InteropServices;
using HdrHistogram;
using Paprika.Crypto;
using Paprika.Data;
using Paprika.Merkle;

namespace Paprika.Store;

/// <summary>
/// Provides capability to report stats for <see cref="DataPage"/>.
/// </summary>
public interface IReporter
{
    void ReportDataUsage(PageType type, int pageLevel, int trimmedNibbles, in SlottedArray array);

    /// <summary>
    /// Reports how many batches ago the page was updated.
    /// </summary>
    void ReportPage(uint ageInBatches, PageType type);

    // void ReportItem(in StoreKey key, ReadOnlySpan<byte> rawData);

    void ReportLeafOverflowCount(byte count);
}

public interface IReporting
{
    void Report(IReporter state, IReporter storage);
}

public class StatisticsReporter(TrieType trieType) : IReporter
{
    public readonly SortedDictionary<int, Level> Levels = new();
    public readonly Dictionary<PageType, int> PageTypes = new();
    public int PageCount;

    public long DataSize;

    public long MerkleBranchSize;
    public long MerkleBranchWithSmallEmpty;
    public long MerkleBranchWithOneChildMissing;
    public long MerkleBranchWithTwoChildrenOnly;
    public long MerkleExtensionSize;
    public long MerkleLeafSize;

    public readonly IntHistogram LeafCapacityLeft = new(10000, 5);
    public readonly IntHistogram LeafOverflowCapacityLeft = new(10000, 5);
    public readonly IntHistogram LeafOverflowCount = new(100, 5);

    public readonly IntHistogram PageAge = new(uint.MaxValue, 5);

    public void ReportDataUsage(PageType type, int pageLevel, int trimmedNibbles, in SlottedArray array)
    {
        if (Levels.TryGetValue(pageLevel, out var lvl) == false)
        {
            lvl = Levels[pageLevel] = new Level();
        }

        PageCount++;

        lvl.Entries.RecordValue(array.Count);

        var capacityLeft = array.CapacityLeft + 1; // to ensure zeroes are handled well
        lvl.CapacityLeft.RecordValue(capacityLeft);

        if (type == PageType.Leaf)
            LeafCapacityLeft.RecordValue(capacityLeft);
        else if (type == PageType.LeafOverflow)
            LeafOverflowCapacityLeft.RecordValue(capacityLeft);

        // analyze data
        foreach (var item in array.EnumerateAll())
        {
            var data = item.RawData;
            var size = data.Length;
            var isMerkle = item.Key.Length + trimmedNibbles < NibblePath.KeccakNibbleCount;

            if (isMerkle)
            {
                if (size > 0)
                {
                    var nodeType = Node.Header.GetTypeFrom(data);
                    switch (nodeType)
                    {
                        case Node.Type.Leaf:
                            MerkleLeafSize += size;
                            break;
                        case Node.Type.Extension:
                            MerkleExtensionSize += size;
                            break;
                        case Node.Type.Branch:
                            MerkleBranchSize += size;
                            var leftover = Node.Branch.ReadFrom(data, out var branch);

                            if (branch.Children.SetCount == 15)
                            {
                                MerkleBranchWithOneChildMissing++;
                            }
                            else if (branch.Children.SetCount == 2)
                            {
                                MerkleBranchWithTwoChildrenOnly++;
                            }

                            var len = leftover.Length % Keccak.Size;
                            if (len > 0)
                            {
                                NibbleSet.Readonly.ReadFrom(leftover[^len..], out var empty);
                                if (empty.SetCount <= 2)
                                {
                                    MerkleBranchWithSmallEmpty++;
                                }
                            }

                            break;
                        default:
                            throw new ArgumentOutOfRangeException();
                    }
                }
            }
            else
            {
                DataSize += size;
            }

            if (!isMerkle && trieType == TrieType.Storage && data.Length > 32)
            {
                throw new Exception(
                    $"Storage, not Merkle node with local key {item.Key.ToString()}, has more than 32 bytes");
            }
        }
    }

    public void ReportPage(uint ageInBatches, PageType type)
    {
        PageAge.RecordValue(ageInBatches);
        var value = PageTypes.GetValueOrDefault(type);
        PageTypes[type] = value + 1;
    }

    public void ReportLeafOverflowCount(byte count)
    {
        LeafOverflowCount.RecordValue(count);
    }

    private const int KeyShift = 8;
    private const int KeyDiff = 1;

    public static string GetNameForSize(int i)
    {
        var type = (DataType)(i & 0xFF);
        var str = type.ToString().Replace(", ", "-");

        if (i >> KeyShift < KeyDiff)
        {
            return str;
        }

        var merkleType = (Node.Type)((i >> KeyShift) - KeyDiff);
        return $"{str}-{merkleType}";
    }

    public class Level
    {
        public readonly IntHistogram ChildCount = new(1000, 5);
        public readonly IntHistogram Entries = new(1000, 5);
        public readonly IntHistogram CapacityLeft = new(10000, 5);
    }
}
