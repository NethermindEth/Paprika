using System.Runtime.InteropServices;
using HdrHistogram;
using Paprika.Data;
using Paprika.Merkle;

namespace Paprika.Store;

/// <summary>
/// Provides capability to report stats for <see cref="DataPage"/>.
/// </summary>
public interface IReporter
{
    void ReportDataUsage(PageType type, int level, int filledBuckets, int entriesPerPage, int capacityLeft);

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

public class StatisticsReporter : IReporter
{
    public readonly SortedDictionary<int, Level> Levels = new();
    public readonly Dictionary<PageType, int> PageTypes = new();
    public int PageCount;

    public readonly IntHistogram LeafCapacityLeft = new(10000, 5);
    public readonly IntHistogram LeafOverflowCapacityLeft = new(10000, 5);
    public readonly IntHistogram LeafOverflowCount = new(100, 5);

    public readonly IntHistogram PageAge = new(uint.MaxValue, 5);

    public void ReportDataUsage(PageType type, int level, int filledBuckets, int entriesPerPage, int capacityLeft)
    {
        if (Levels.TryGetValue(level, out var lvl) == false)
        {
            lvl = Levels[level] = new Level();
        }

        PageCount++;

        lvl.ChildCount.RecordValue(filledBuckets);

        lvl.Entries.RecordValue(entriesPerPage);
        lvl.CapacityLeft.RecordValue(capacityLeft);

        if (type == PageType.Leaf)
            LeafCapacityLeft.RecordValue(capacityLeft);
        else if (type == PageType.LeafOverflow)
            LeafOverflowCapacityLeft.RecordValue(capacityLeft);
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

    // public void ReportItem(in StoreKey key, ReadOnlySpan<byte> rawData)
    // {
    //     var index = GetKey(key, rawData);
    //
    //     // total size
    //     const int slottedArraySlot = 4;
    //     var keyEstimatedLength = key.Payload.Length + slottedArraySlot;
    //     var total = rawData.Length + keyEstimatedLength;
    //
    //     ref var value = ref CollectionsMarshal.GetValueRefOrAddDefault(Sizes, index, out _);
    //     value += total;
    //
    //     if (!SizeHistograms.TryGetValue(index, out var histogram))
    //     {
    //         SizeHistograms[index] = histogram = new IntHistogram(1000, 3);
    //     }
    //
    //     histogram.RecordValue(total);
    // }

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
