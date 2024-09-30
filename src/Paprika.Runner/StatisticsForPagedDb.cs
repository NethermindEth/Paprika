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
                            BuildReport(stats.Storage, "Storage")),
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

    private static Layout BuildReport(StatisticsVisitor.Stats stats, string name)
    {
        var up = new Layout("up");
        var sizes = new Layout("down");
        //var leafs = new Layout("leafs");

        var layout = new Layout().SplitRows(up, sizes /*, leafs*/);

        // var totalMerkle = stats.MerkleBranchSize + stats.MerkleExtensionSize + stats.MerkleLeafSize;

        var general =
            $"Size total: {Page.FormatAsGb(stats.PageCount)}\n";
        //+
        // $" Merkle:    {ToGb(totalMerkle):F2}GB:\n" +
        // $"  Branches: {ToGb(stats.MerkleBranchSize):F2}GB\n" +
        // $"  Ext.:     {ToGb(stats.MerkleExtensionSize):F2}GB\n" +
        // $"  Leaf:     {ToGb(stats.MerkleLeafSize):F2}GB\n" +
        // $" Data:      {ToGb(stats.DataSize):F2}GB\n" +
        // "---\n" +
        // $" Branches with small empty set: {stats.MerkleBranchWithSmallEmpty}\n" +
        // $" Branches with 15 children: {stats.MerkleBranchWithOneChildMissing}\n" +
        // $" Branches with 3 or less children: {stats.MerkleBranchWithThreeChildrenOrLess}\n";

        up.Update(new Panel(general).Header($"General stats for {name}").Expand());

        var t = new Table();
        t.AddColumn(new TableColumn("Depth"));
        t.AddColumn(new TableColumn("Page count"));
        t.AddColumn(new TableColumn("Leaf page count"));
        t.AddColumn(new TableColumn("Overflow page count"));

        // t.AddColumn(new TableColumn("Child page count"));
        //
        // t.AddColumn(new TableColumn("Entries in page"));
        // t.AddColumn(new TableColumn("Capacity left (bytes)"));

        var countByLevel = stats.PageCountPerNibblePathDepth.ToArray();
        Array.Sort(countByLevel, (a, b) => a.Key.CompareTo(b.Key));

        foreach (var (depth, count) in countByLevel)
        {
            var leafPageCount = stats.LeafPageCountPerNibblePathDepth.GetValueOrDefault(depth, 0);
            var overflowCount = stats.OverflowPageCountPerNibblePathDepth.GetValueOrDefault(depth, 0);

            t.AddRow(
                new Text(depth.ToString()),
                new Text(count.ToString()),
                new Text(leafPageCount.ToString()),
                new Text(overflowCount.ToString())
            );

            // var entries = level.Entries;
            // var capacity = level.CapacityLeft;
            //
            // t.AddRow(
            //     new Text(key.ToString()),
            //     WriteHistogram(level.ChildCount),
            //     WriteHistogram(entries),
            //     WriteHistogram(capacity));
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