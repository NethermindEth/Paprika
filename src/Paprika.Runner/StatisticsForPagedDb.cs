using System.Runtime.InteropServices;
using System.Text;
using HdrHistogram;
using Paprika.Merkle;
using Paprika.Store;
using Spectre.Console;
using Spectre.Console.Rendering;

namespace Paprika.Runner;

public static class StatisticsForPagedDb
{
    public static void Report(Layout reportTo, IVisitable read, IPageResolver resolver)
    {
        const string txt = "Gathering statistics...";
        const string header = "Paprika tree statistics";
        reportTo.Update(new Panel(txt).Header(header).Expand());

        try
        {
            var stats = new StatisticsVisitor(resolver);

            stats.TotalVisitedChanged += (_, __) =>
            {
                var total = stats.TotalVisited;
                const long pagePer128MB = 128 * 1024 * 1024 / Page.PageSize;

                if (total % pagePer128MB == 0)
                {
                    var size = Page.FormatAsGb(total);
                    reportTo.Update(new Panel($"{txt} Analyzed so far: {size}").Header(header).Expand());
                }
            };

            read.Accept(stats);

            var report = new Layout()
                .SplitRows(
                    new Layout("top")
                        .SplitColumns(
                            BuildReport(stats.State, "State"),
                            BuildReport(stats.Storage, "Storage", stats.StorageFanOutLevels)),
                    new Layout("bottom")
                        .Update(new Panel(new Paragraph(
                            $"- pages used for id mapping: {Page.FormatAsGb(stats.Ids.PageCount)}\n" +
                            $"- total pages abandoned:     {Page.FormatAsGb(stats.AbandonedCount)}\n" +
                            $"- total pages visited:       {Page.FormatAsGb(stats.TotalVisited)}\n" +
                            "")).Header("Other stats").Expand())
                );

            reportTo.Update(new Panel(report).Header(header).Expand());
        }
        catch (Exception e)
        {
            var paragraph = new Paragraph();

            var style = new Style().Foreground(Color.Red);
            paragraph.Append(e.Message, style);
            paragraph.Append(e.StackTrace!, style);

            reportTo.Update(new Panel(paragraph).Header(header).Expand());
        }
    }

    private static Layout BuildReport(StatisticsVisitor.Stats stats, string name, int[]? fanOutLevels = null)
    {
        var up = new Layout("up");
        var sizes = new Layout("down");

        var layout = new Layout().SplitRows(up, sizes);


        var general =
            $"Size total: {Page.FormatAsGb(stats.PageCount)}\n";

        up.Update(new Panel(general).Header($"General stats for {name}").Expand());

        var t = new Table();
        t.AddColumn(new TableColumn("Depth"));
        t.AddColumn(new TableColumn("Page count"));
        t.AddColumn(new TableColumn("Leaf page count"));
        t.AddColumn(new TableColumn("Overflow page count"));
        t.AddColumn(new TableColumn($"{nameof(DataPage)} % usage (P50)"));

        if (fanOutLevels != null)
        {
            var max = fanOutLevels.AsSpan().LastIndexOfAnyExcept(0) + 1;

            for (int depth = 0; depth < max; depth++)
            {
                var count = fanOutLevels[depth];

                t.AddRow(
                    new Text($"{nameof(StorageFanOut)}, lvl: {depth}"),
                    new Text(count.ToString()),
                    new Text("-"),
                    new Text("-"),
                    new Text("-")
                );
            }
        }


        var maxDepth = stats.PageCountPerNibblePathDepth.AsSpan().LastIndexOfAnyExcept(0) + 1;

        for (int depth = 0; depth < maxDepth; depth++)
        {
            var count = stats.PageCountPerNibblePathDepth[depth];
            var leafPageCount = stats.LeafPageCountPerNibblePathDepth[depth];
            var overflowCount = stats.OverflowPageCountPerNibblePathDepth[depth];

            var usage = "-";
            var histogram = stats.InnerPagePercentageUsed[depth];
            if (histogram != null)
            {
                try
                {
                    usage = histogram.GetValueAtPercentile(50).ToString();
                }
                catch
                {
                    // ignored
                }
            }

            t.AddRow(
                new Text(depth.ToString()),
                new Text(count.ToString()),
                new Text(leafPageCount.ToString()),
                new Text(overflowCount.ToString()),
                new Text(usage)
            );
        }

        sizes.Update(t.Expand());

        // var leafsTable = new Table();
        // leafsTable.AddColumn(new TableColumn("Leaf capacity left"));
        // leafsTable.AddColumn(new TableColumn("Leaf->Overflow capacity left"));
        // leafsTable.AddColumn(new TableColumn("Leaf->Overflow count"));
        //
        // leafsTable.AddRow(
        //     WriteHistogram(stats.LeafCapacityLeft),
        //     WriteHistogram(stats.LeafOverflowCapacityLeft),
        //     WriteHistogram(stats.LeafOverflowCount));
        //
        // leafs.Update(leafsTable.Expand());

        return layout;
    }

    // private static IRenderable WriteSizePerType(Dictionary<int, long> sizes, string prefix)
    // {
    //     var sb = new StringBuilder();
    //     sb.Append(prefix);
    //
    //     foreach (var (index, count) in sizes)
    //     {
    //         var name = StatisticsReporter.GetNameForSize(index);
    //         var gb = ((double)count) / 1024 / 1024 / 1024;
    //         sb.Append($" {name}:{gb:F1}GB");
    //     }
    //
    //     return new Markup(sb.ToString());
    // }

    // private static IRenderable WriteHistogramSizePerType(Dictionary<int, IntHistogram> sizes, string prefix)
    // {
    //     var sb = new StringBuilder();
    //     sb.Append(prefix);
    //
    //     foreach (var (index, histogram) in sizes)
    //     {
    //         var name = StatisticsReporter.GetNameForSize(index);
    //         var median = histogram.GetValueAtPercentile(50);
    //         sb.Append($" {name}: {median} bytes");
    //     }
    //
    //     return new Markup(sb.ToString());
    // }
    //
    // private static IRenderable WriteHistogram(HistogramBase histogram, string prefix = "")
    // {
    //     var sb = new StringBuilder();
    //
    //     sb.Append(prefix);
    //     foreach (var percentile in Percentiles)
    //     {
    //         sb.Append(Percentile(percentile.value, percentile.color));
    //     }
    //
    //     return new Markup(sb.ToString());
    //
    //     string Percentile(int percentile, string color)
    //     {
    //         try
    //         {
    //             var value = histogram.GetValueAtPercentile(percentile);
    //             return $"[{color}]P{percentile}: {value,2}[/] ";
    //         }
    //         catch (Exception e)
    //         {
    //             return $"[{color}]P{percentile}: N/A[/] ";
    //         }
    //     }
    // }
    //
    // private static readonly (int value, string color)[] Percentiles =
    // {
    //     new(50, "green"),
    //     new(90, "yellow"),
    //     new(95, "red"),
    // };
}