using System.Text;
using HdrHistogram;
using Paprika.Store;
using Spectre.Console;
using Spectre.Console.Rendering;

namespace Paprika.Runner;

public static class StatisticsForPagedDb
{
    public static void Report(Layout reportTo, IReporting read)
    {
        reportTo.Update(new Panel("Gathering statistics...").Header("Paprika tree statistics").Expand());

        try
        {
            var state = new StatisticsReporter();
            var storage = new StatisticsReporter();

            read.Report(state, storage);

            var report = new Layout()
                .SplitColumns(
                    BuildReport(state, "State"),
                    BuildReport(storage, "Storage"));

            reportTo.Update(new Panel(report).Header("Paprika tree statistics").Expand());
        }
        catch (Exception e)
        {
            var paragraph = new Paragraph();

            var style = new Style().Foreground(Color.Red);
            paragraph.Append(e.Message, style);
            paragraph.Append(e.StackTrace!, style);

            reportTo.Update(new Panel(paragraph).Header("Paprika tree statistics").Expand());
        }
    }

    private static Layout BuildReport(StatisticsReporter reporter, string name)
    {
        var up = new Layout("up");
        var down = new Layout("down");

        var layout = new Layout().SplitRows(up, down);

        var general = $"Number of pages: {reporter.PageCount}";
        up.Update(new Panel(general).Header($"General stats for {name}").Expand());

        var t = new Table();
        t.AddColumn(new TableColumn("Depth"));
        t.AddColumn(new TableColumn("Child page count"));

        t.AddColumn(new TableColumn("Entries in page"));
        t.AddColumn(new TableColumn("Capacity left (bytes)"));

        foreach (var (key, level) in reporter.Levels)
        {
            var entries = level.Entries;
            var capacity = level.CapacityLeft;

            t.AddRow(
                new Text(key.ToString()),
                WriteHistogram(level.ChildCount),
                WriteHistogram(entries),
                WriteHistogram(capacity));
        }

        down.Update(t.Expand());

        return layout;
    }

    private static IRenderable WriteSizePerType(Dictionary<int, long> sizes, string prefix)
    {
        var sb = new StringBuilder();
        sb.Append(prefix);

        foreach (var (index, count) in sizes)
        {
            var name = StatisticsReporter.GetNameForSize(index);
            var gb = ((double)count) / 1024 / 1024 / 1024;
            sb.Append($" {name}:{gb:F1}GB");
        }

        return new Markup(sb.ToString());
    }

    private static IRenderable WriteHistogramSizePerType(Dictionary<int, IntHistogram> sizes, string prefix)
    {
        var sb = new StringBuilder();
        sb.Append(prefix);

        foreach (var (index, histogram) in sizes)
        {
            var name = StatisticsReporter.GetNameForSize(index);
            var median = histogram.GetValueAtPercentile(50);
            sb.Append($" {name}: {median} bytes");
        }

        return new Markup(sb.ToString());
    }

    private static IRenderable WriteHistogram(HistogramBase histogram, string prefix = "")
    {
        var sb = new StringBuilder();

        sb.Append(prefix);
        foreach (var percentile in Percentiles)
        {
            sb.Append(Percentile(percentile.value, percentile.color));
        }

        return new Markup(sb.ToString());

        string Percentile(int percentile, string color)
        {
            try
            {
                var value = histogram.GetValueAtPercentile(percentile);
                return $"[{color}]P{percentile}: {value,2}[/] ";
            }
            catch (Exception e)
            {
                return $"[{color}]P{percentile}: N/A[/] ";
            }
        }
    }

    private static readonly (int value, string color)[] Percentiles =
    {
        new(50, "green"),
        new(90, "yellow"),
        new(95, "red"),
    };
}