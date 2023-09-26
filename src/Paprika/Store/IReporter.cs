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

    void ReportItem(in Key key, ReadOnlySpan<byte> rawData);
}

public class StatisticsReporter : IReporter
{
    public readonly SortedDictionary<int, Level> Levels = new();
    public readonly Dictionary<PageType, int> PageTypes = new();
    public int PageCount;

    private const int SizeCount = (int)(DataType.Merkle + (byte)Node.Type.Branch) + 1;
    public readonly long[] Sizes = new long[SizeCount];
    public readonly SortedDictionary<int, IntHistogram> SizeHistograms = new();

    public readonly IntHistogram PageAge = new(uint.MaxValue, 5);

    public void ReportDataUsage(PageType type, int level, int filledBuckets, int entriesPerPage, int capacityLeft)
    {
        if (Levels.TryGetValue(level, out var lvl) == false)
        {
            lvl = Levels[level] = new Level();
        }

        PageCount++;

        lvl.ChildCount.RecordValue(filledBuckets);

        if (type == PageType.Standard)
        {
            lvl.StandardEntries.RecordValue(entriesPerPage);
            lvl.StandardCapacityLeft.RecordValue(capacityLeft);
        }
        else
        {
            lvl.PrefixedEntries.RecordValue(entriesPerPage);
            lvl.PrefixedCapacityLeft.RecordValue(capacityLeft);
        }
    }

    public void ReportPage(uint ageInBatches, PageType type)
    {
        PageAge.RecordValue(ageInBatches);
        var value = PageTypes.GetValueOrDefault(type);
        PageTypes[type] = value + 1;
    }

    public void ReportItem(in Key key, ReadOnlySpan<byte> rawData)
    {
        var index = GetKey(key, rawData);

        // total size
        const int slottedArraySlot = 4;
        var keyEstimatedLength = key.Path.MaxByteLength + key.StoragePath.MaxByteLength + slottedArraySlot;
        var total = rawData.Length + keyEstimatedLength;
        Sizes[index] += total;

        if (!SizeHistograms.TryGetValue(index, out var histogram))
        {
            SizeHistograms[index] = histogram = new IntHistogram(1000, 3);
        }

        histogram.RecordValue(total);
    }

    private static int GetKey(in Key key, in ReadOnlySpan<byte> data)
    {
        switch (key.Type)
        {
            case DataType.Account:
                return (int)DataType.Account;
            case DataType.StorageCell:
                return (int)DataType.StorageCell;
            case DataType.Merkle:
                Node.Header.ReadFrom(data, out var header);
                return (int)(DataType.Merkle + (byte)header.NodeType);
        }

        return 0;
    }

    public static string GetNameForSize(int i)
    {
        return i switch
        {
            0 => nameof(DataType.Account),
            1 => nameof(DataType.StorageCell),
            _ => $"{nameof(DataType.Merkle)}-{((Node.Type)(i - 2)).ToString()}", // merkle
        };
    }

    public class Level
    {
        public readonly IntHistogram ChildCount = new(1000, 5);

        public readonly IntHistogram StandardEntries = new(1000, 5);
        public readonly IntHistogram StandardCapacityLeft = new(10000, 5);

        public readonly IntHistogram PrefixedEntries = new(1000, 5);
        public readonly IntHistogram PrefixedCapacityLeft = new(10000, 5);
    }
}