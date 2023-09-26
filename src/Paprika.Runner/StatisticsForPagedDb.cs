using System.Text;
using HdrHistogram;
using Paprika.Store;
using Spectre.Console;
using Spectre.Console.Rendering;

namespace Paprika.Runner;

public static class StatisticsForPagedDb
{
    public static void Report(Layout reportTo, IReadOnlyBatch read, bool splitPageTypes = false)
    {
        reportTo.Update(new Panel("Gathering statistics...").Header("Paprika tree statistics").Expand());

        try
        {
            var stats = new StatisticsReporter();
            read.Report(stats);

            var levelStats = new Table();
            levelStats.AddColumn(new TableColumn("Level of Paprika tree"));
            levelStats.AddColumn(new TableColumn("Child page count"));

            if (splitPageTypes)
            {
                levelStats.AddColumn(new TableColumn("Entries in page (Standard)"));
                levelStats.AddColumn(new TableColumn("Capacity left (Standard)"));
                levelStats.AddColumn(new TableColumn("Entries in page (Prefixed)"));
                levelStats.AddColumn(new TableColumn("Capacity left (Prefixed)"));
            }
            else
            {
                levelStats.AddColumn(new TableColumn("Entries in page"));
                levelStats.AddColumn(new TableColumn("Capacity left (bytes)"));
            }

            foreach (var (key, level) in stats.Levels)
            {
                if (splitPageTypes)
                {
                    levelStats.AddRow(
                        new Text(key.ToString()),
                        WriteHistogram(level.ChildCount),
                        WriteHistogram(level.StandardEntries),
                        WriteHistogram(level.StandardCapacityLeft),
                        WriteHistogram(level.PrefixedEntries),
                        WriteHistogram(level.PrefixedCapacityLeft));
                }
                else
                {
                    var entries = level.PrefixedEntries;
                    entries.Add(level.StandardEntries);

                    var capacity = level.PrefixedCapacityLeft;
                    capacity.Add(level.StandardCapacityLeft);

                    levelStats.AddRow(
                        new Text(key.ToString()),
                        WriteHistogram(level.ChildCount),
                        WriteHistogram(entries),
                        WriteHistogram(capacity));
                }
            }

            var mb = (long)stats.PageCount * Page.PageSize / 1024 / 1024;

            var types = string.Join(", ", stats.PageTypes.Select(kvp => $"{kvp.Key}: {kvp.Value}"));

            // histogram description
            var sb = new StringBuilder();
            sb.Append("Histogram percentiles: ");
            foreach (var percentile in Percentiles)
            {
                sb.Append($"[{percentile.color}]P{percentile.value}: {percentile.value}th percentile [/] ");
            }

            var report = new Layout()
                .SplitRows(
                    new Layout(
                            new Rows(
                                new Markup(sb.ToString()),
                                new Text(""),
                                new Text("General stats:"),
                                new Text($"1. Size of this Paprika tree: {mb}MB"),
                                new Text($"2. Types of pages: {types}"),
                                WriteHistogram(stats.PageAge, "3. Age of pages: "),
                                WriteSizePerType(stats.Sizes, "3. Size per entry type:")
                                ))
                        .Size(8),
                    new Layout(levelStats.Expand()));

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

    private static IRenderable WriteSizePerType(long[] sizes, string prefix)
    {
        var sb = new StringBuilder();
        sb.Append(prefix);

        for (var i = 0; i < sizes.Length; i++)
        {
            var mb = sizes[i] / 1024 / 1024;
            var name = StatisticsReporter.GetNameForSize(i);

            sb.Append($" {name}:{mb}MB");
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