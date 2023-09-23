﻿using System.Text;
using HdrHistogram;
using Paprika.Store;
using Spectre.Console;
using Spectre.Console.Rendering;

namespace Paprika.Runner;

public static class StatisticsForPagedDb
{
    public static void Report(Layout reportTo, IReadOnlyBatch read)
    {
        reportTo.Update(new Panel("Gathering statistics...").Header("Paprika tree statistics").Expand());

        var stats = new StatisticsReporter();
        read.Report(stats);
        var table = new Table();

        table.AddColumn(new TableColumn("Level of Paprika tree"));
        table.AddColumn(new TableColumn("Child page count"));
        table.AddColumn(new TableColumn("Entries in page"));

        foreach (var (key, level) in stats.Levels)
        {
            table.AddRow(
                new Text(key.ToString()),
                WriteHistogram(level.ChildCount),
                WriteHistogram(level.Entries));
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
                            WriteHistogram(stats.PageAge, "2. Age of pages: ")))
                    .Size(7),
                new Layout(table.Expand()));

        reportTo.Update(new Panel(report).Header("Paprika tree statistics").Expand());
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
            var value = histogram.GetValueAtPercentile(percentile);
            return $"[{color}]P{percentile}: {value,2}[/] ";
        }
    }

    private static readonly (int value, string color)[] Percentiles =
    {
        new(50, "green"),
        new(90, "yellow"),
        new(95, "red"),
    };
}