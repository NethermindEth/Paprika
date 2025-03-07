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

        public readonly int[] DataPagePageCountPerNibblePathDepth = new int[Levels];
        public readonly int[] BottomPageCountPerNibblePathDepth = new int[Levels];

        public int CurrentLevel = 0;

        /// <summary>
        /// A histogram of used space in inner <see cref="DataPage"/>.
        /// </summary>
        public readonly IntHistogram?[] DataPagePercentageUsed = new IntHistogram[Levels];

        /// <summary>
        /// A histogram of used space in leaf <see cref="BottomPage"/>.
        /// </summary>
        public readonly IntHistogram?[] BottomPagePercentageUsed = new IntHistogram[Levels];

        public void ReportDataPageMap(int length, in SlottedArray map) =>
            ReportMap(DataPagePercentageUsed, length, map);

        public void ReportBottomPageMap(int length, in SlottedArray map) =>
            ReportMap(BottomPagePercentageUsed, length, map);

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

        var t = typeof(TPage);
        if (t == typeof(StorageFanOut.Level1Page))
        {
            StorageFanOutLevels[1] += 1;
            return NoopDisposable.Instance;
        }

        var lvl = _current.CurrentLevel;

        var p = page.AsPage();

        switch (p.Header.PageType)
        {
            case PageType.DataPage:
                _current.DataPagePageCountPerNibblePathDepth[lvl] += 1;
                _current.ReportDataPageMap(lvl, new DataPage(p).Map);
                break;
            case PageType.Bottom:
                _current.BottomPageCountPerNibblePathDepth[lvl] += 1;
                _current.ReportBottomPageMap(lvl, new BottomPage(p).Map);
                break;
            case PageType.ChildBottom:
                _current.BottomPageCountPerNibblePathDepth[lvl] += 1;
                _current.ReportBottomPageMap(lvl, new ChildBottomPage(p).Map);
                break;
            case PageType.StateRoot:
                break;
            default:
                throw new Exception($"Not handled type {p.Header.PageType}");
        }

        return new LevelUp(_current);
    }

    private sealed class LevelUp : IDisposable
    {
        private readonly Stats _stats;

        public LevelUp(Stats stats)
        {
            _stats = stats;
            _stats.CurrentLevel++;
        }

        public void Dispose()
        {
            _stats.CurrentLevel--;
        }
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