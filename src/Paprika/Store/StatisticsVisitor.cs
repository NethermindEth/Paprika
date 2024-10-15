using HdrHistogram;
using Paprika.Data;

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

        /// <summary>
        /// A histogram of used space in inner <see cref="DataPage"/>.
        /// </summary>
        public readonly IntHistogram?[] InnerDataPagePercentageUsed = new IntHistogram[Levels];

        /// <summary>
        /// A histogram of used space in leaf <see cref="DataPage"/>.
        /// </summary>
        public readonly IntHistogram?[] LeafDataPagePercentageUsed = new IntHistogram[Levels];

        /// <summary>
        /// A histogram of used space in leaf <see cref="LeafOverflowPage"/>.
        /// </summary>
        public readonly IntHistogram?[] OverflowPagePercentageUsed = new IntHistogram[Levels];

        public void ReportInnerDataPageMap(int length, in SlottedArray map) =>
            ReportMap(InnerDataPagePercentageUsed, length, map);

        public void ReportLeafDataPageMap(int length, in SlottedArray map) =>
            ReportMap(LeafDataPagePercentageUsed, length, map);

        public void ReportOverflowPageMap(int length, in SlottedArray map) =>
            ReportMap(OverflowPagePercentageUsed, length, map);

        private static void ReportMap(IntHistogram?[] histograms, int length, in SlottedArray map)
        {
            var histogram = histograms[length] ??= new IntHistogram(100, 5);
            var percentage = (int)(map.CalculateActualSpaceUsed() * 100);

            histogram.RecordValue(percentage);
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
                    var map = dataPage.Map;

                    if (dataPage.IsLeaf)
                    {
                        _current.LeafPageCountPerNibblePathDepth[length] += 1;
                    }

                    if (dataPage.IsLeaf == false)
                    {
                        _current.ReportInnerDataPageMap(length, map);
                    }
                    else
                    {
                        _current.ReportLeafDataPageMap(length, map);
                    }

                    break;
                case PageType.LeafOverflow:
                    _current.OverflowPageCountPerNibblePathDepth[length] += 1;
                    _current.ReportOverflowPageMap(length, new LeafOverflowPage(p).Map);
                    break;
            }
        }

        return NoopDisposable.Instance;
    }

    public IDisposable On<TPage>(TPage page, DbAddress addr) where TPage : unmanaged, IPage
    {
        IncTotalVisited();

        if (typeof(TPage) == typeof(AbandonedPage))
        {
            AbandonedCount += new AbandonedPage(page.AsPage()).CountPages();
        }

        return NoopDisposable.Instance;
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
                return NoopDisposable.Instance;
            default:
                throw new NotImplementedException($"Not implemented for {name}");
        }
    }

    private sealed class ModeScope(StatisticsVisitor visitor, Stats previous) : IDisposable
    {
        public void Dispose() => visitor._current = previous;
    }
}