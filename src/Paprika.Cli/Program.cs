using Paprika.Runner;
using Paprika.Store;
using Spectre.Console;
using Spectre.Console.Cli;

var app = new CommandApp();
app.Configure(cfg =>
{
    cfg.AddCommand<GatherStatistics>("stats");
    cfg.SetExceptionHandler(ex =>
    {
        AnsiConsole.WriteException(ex, ExceptionFormats.ShortenEverything);
        return -99;
    });
});

await app.RunAsync(args);

public class StatisticsSettings : CommandSettings
{
    [CommandArgument(0, "<path>")]
    public string Path { get; set; }

    [CommandArgument(1, "<size>")]
    public byte Size { get; set; }

    [CommandArgument(1, "<historyDepth>")]
    public byte HistoryDepth { get; set; }
}

public class GatherStatistics : Command<StatisticsSettings>
{
    public override int Execute(CommandContext context, StatisticsSettings settings)
    {
        const long Gb = 1024 * 1024 * 1024;

        using var db = PagedDb.MemoryMappedDb(settings.Size * Gb, settings.HistoryDepth, settings.Path, false);
        using var read = db.BeginReadOnlyBatch();

        var stats = new Layout("Stats");

        AnsiConsole.Write("Gathering stats...");

        StatisticsForPagedDb.Report(stats, read);

        AnsiConsole.Write(stats);

        return 0;
    }
}