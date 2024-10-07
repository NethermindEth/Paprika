using System.Runtime.InteropServices;
using HdrHistogram;
using Paprika.Crypto;
using Paprika.Data;
using Paprika.Merkle;

namespace Paprika.Store;

public class StatisticsVisitor : IPageVisitor
{
    private readonly IPageResolver _resolver;

    public readonly Stats State;
    public readonly Stats Ids;
    public readonly Stats Storage;

    public readonly int[] StorageFanOutLevels = new int[StorageFanOut.LevelCount + 1];

    public int AbandonedCount;
    public int TotalVisited;

    private Stats _current;

    public StatisticsVisitor(IPageResolver resolver)
    {
        _resolver = resolver;
        State = new Stats();
        Ids = new Stats();
        Storage = new Stats();

        _current = State;
    }

    public class Stats
    {
        public const int Levels = 32;

        public readonly int[] PageCountPerNibblePathDepth = new int[Levels];
        public readonly int[] LeafPageCountPerNibblePathDepth = new int[Levels];
        public readonly int[] OverflowPageCountPerNibblePathDepth = new int[Levels];

        public void ReportMap(scoped ref NibblePath.Builder prefix, in SlottedArray map)
        {
        }

        public int PageCount { get; set; }
    }

    public event EventHandler? TotalVisitedChanged;

    public IDisposable On<TPage>(scoped ref NibblePath.Builder prefix, TPage page, DbAddress addr)
        where TPage : unmanaged, IPage
    {
        IncTotalVisited();

        _current.PageCount++;

        if (typeof(TPage) == typeof(StorageFanOut.Level1Page))
        {
            StorageFanOutLevels[1] += 1;
        }
        else if (typeof(TPage) == typeof(StorageFanOut.Level2Page))
        {
            StorageFanOutLevels[2] += 1;
        }
        else if (typeof(TPage) == typeof(StorageFanOut.Level3Page))
        {
            StorageFanOutLevels[3] += 1;
        }
        else
        {
            var length = prefix.Current.Length;

            _current.PageCountPerNibblePathDepth[length] += 1;

            var p = page.AsPage();
            switch (p.Header.PageType)
            {
                case PageType.DataPage:
                    var dataPage = new DataPage(p);
                    if (dataPage.IsLeaf)
                    {
                        _current.LeafPageCountPerNibblePathDepth[length] += 1;
                    }

                    _current.ReportMap(ref prefix, dataPage.Map);
                    break;
                case PageType.LeafOverflow:
                    _current.OverflowPageCountPerNibblePathDepth[length] += 1;
                    _current.ReportMap(ref prefix, new LeafOverflowPage(p).Map);
                    break;
            }
        }

        return Disposable.Instance;
    }

    public IDisposable On<TPage>(TPage page, DbAddress addr) where TPage : unmanaged, IPage
    {
        IncTotalVisited();

        if (typeof(TPage) == typeof(AbandonedPage))
        {
            AbandonedCount += new AbandonedPage(page.AsPage()).CountPages();
        }

        return Disposable.Instance;
    }

    private void IncTotalVisited()
    {
        TotalVisited++;
        TotalVisitedChanged?.Invoke(this, EventArgs.Empty);
    }

    public IDisposable Scope(string name)
    {
        var previous = _current;

        switch (name)
        {
            case StorageFanOut.ScopeIds:
                _current = Ids;
                return new ModeScope(this, previous);
            case StorageFanOut.ScopeStorage:
                _current = Storage;
                return new ModeScope(this, previous);
            case nameof(StorageFanOut):
                return Disposable.Instance;
            default:
                throw new NotImplementedException($"Not implemented for {name}");
        }
    }

    private sealed class ModeScope(StatisticsVisitor visitor, Stats previous) : IDisposable
    {
        public void Dispose() => visitor._current = previous;
    }
}

//
// public class StatisticsReporter(TrieType trieType) : IReporter
// {
//     public readonly SortedDictionary<int, Level> Levels = new();
//     public readonly Dictionary<PageType, int> PageTypes = new();
//     public int PageCount;
//
//     public long DataSize;
//
//     public long MerkleBranchSize;
//     public long MerkleBranchWithSmallEmpty;
//     public long MerkleBranchWithOneChildMissing;
//     public long MerkleBranchWithThreeChildrenOrLess;
//     public long MerkleExtensionSize;
//     public long MerkleLeafSize;
//
//     public readonly IntHistogram LeafCapacityLeft = new(10000, 5);
//     public readonly IntHistogram LeafOverflowCapacityLeft = new(10000, 5);
//     public readonly IntHistogram LeafOverflowCount = new(100, 5);
//
//     public readonly IntHistogram PageAge = new(uint.MaxValue, 5);
//
//     public void ReportDataUsage(PageType type, int pageLevel, int trimmedNibbles, in SlottedArray array)
//     {
//         if (Levels.TryGetValue(pageLevel, out var lvl) == false)
//         {
//             lvl = Levels[pageLevel] = new Level();
//         }
//
//         PageCount++;
//
//         lvl.Entries.RecordValue(array.Count);
//
//         var capacityLeft = array.CapacityLeft + 1; // to ensure zeroes are handled well
//         lvl.CapacityLeft.RecordValue(capacityLeft);
//
//         if (type == PageType.LeafOverflow)
//             LeafOverflowCapacityLeft.RecordValue(capacityLeft);
//
//         // analyze data
//         foreach (var item in array.EnumerateAll())
//         {
//             var data = item.RawData;
//             var size = data.Length;
//             var isMerkle = item.Key.Length + trimmedNibbles < NibblePath.KeccakNibbleCount;
//
//             if (isMerkle)
//             {
//                 if (size > 0)
//                 {
//                     var nodeType = Node.Header.GetTypeFrom(data);
//                     switch (nodeType)
//                     {
//                         case Node.Type.Leaf:
//                             MerkleLeafSize += size;
//                             break;
//                         case Node.Type.Extension:
//                             MerkleExtensionSize += size;
//                             break;
//                         case Node.Type.Branch:
//                             MerkleBranchSize += size;
//                             var leftover = Node.Branch.ReadFrom(data, out var branch);
//
//                             if (branch.Children.SetCount == 15)
//                             {
//                                 MerkleBranchWithOneChildMissing++;
//                             }
//                             else if (branch.Children.SetCount <= 3)
//                             {
//                                 MerkleBranchWithThreeChildrenOrLess++;
//                             }
//
//                             var len = leftover.Length % Keccak.Size;
//                             if (len > 0)
//                             {
//                                 NibbleSet.Readonly.ReadFrom(leftover[^len..], out var empty);
//                                 if (empty.SetCount <= 2)
//                                 {
//                                     MerkleBranchWithSmallEmpty++;
//                                 }
//                             }
//
//                             break;
//                         default:
//                             throw new ArgumentOutOfRangeException();
//                     }
//                 }
//             }
//             else
//             {
//                 DataSize += size;
//             }
//
//             if (!isMerkle && trieType == TrieType.Storage && data.Length > 32)
//             {
//                 throw new Exception(
//                     $"Storage, not Merkle node with local key {item.Key.ToString()}, has more than 32 bytes");
//             }
//         }
//     }
//
//     public void ReportPage(uint ageInBatches, PageType type)
//     {
//         PageAge.RecordValue(ageInBatches);
//         var value = PageTypes.GetValueOrDefault(type);
//         PageTypes[type] = value + 1;
//     }
//
//     public void ReportLeafOverflowCount(byte count)
//     {
//         LeafOverflowCount.RecordValue(count);
//     }
//
//     private const int KeyShift = 8;
//     private const int KeyDiff = 1;
//
//     public static string GetNameForSize(int i)
//     {
//         var type = (DataType)(i & 0xFF);
//         var str = type.ToString().Replace(", ", "-");
//
//         if (i >> KeyShift < KeyDiff)
//         {
//             return str;
//         }
//
//         var merkleType = (Node.Type)((i >> KeyShift) - KeyDiff);
//         return $"{str}-{merkleType}";
//     }
//
//     public class Level
//     {
//         public readonly IntHistogram ChildCount = new(1000, 5);
//         public readonly IntHistogram Entries = new(1000, 5);
//         public readonly IntHistogram CapacityLeft = new(10000, 5);
//     }
// }