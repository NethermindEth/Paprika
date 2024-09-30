using Paprika.Cli;
using Spectre.Console;
using Spectre.Console.Cli;

var app = new CommandApp();
app.Configure(cfg =>
{
    cfg.AddCommand<StatisticsSettings.Command>("stats");
    cfg.AddCommand<StorageVisitSettings.Command>("storage");

    cfg.SetExceptionHandler(ex =>
    {
        AnsiConsole.WriteException(ex, ExceptionFormats.ShortenEverything);
        return -99;
    });
});

await app.RunAsync(args);