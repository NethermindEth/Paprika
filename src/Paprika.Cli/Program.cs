using System.Diagnostics;
using Paprika.Chain;
using Paprika.Merkle;
using Paprika.Store;
using Paprika.Store.PageManagers;
using Spectre.Console;
using Spectre.Console.Cli;

var app = new CommandApp();
app.Configure(cfg =>
{
    cfg.AddCommand<ValidateRootHashesSettingsCommand>("validate");
    cfg.SetExceptionHandler(ex =>
    {
        AnsiConsole.WriteException(ex, ExceptionFormats.ShortenEverything);
        return -99;
    });
});

await app.RunAsync(args);

public class ValidateRootHashesSettings : CommandSettings
{
    [CommandArgument(0, "<path>")]
    public string Path { get; set; }

    [CommandArgument(1, "<historyDepth>")]
    public byte HistoryDepth { get; set; }
}

public class ValidateRootHashesSettingsCommand : AsyncCommand<ValidateRootHashesSettings>
{
    public override async Task<int> ExecuteAsync(CommandContext context, ValidateRootHashesSettings settings)
    {
        var file = MemoryMappedPageManager.GetPaprikaFilePath(settings.Path);
        if (File.Exists(file) == false)
            throw new FileNotFoundException("Paprika file was not found", file);

        var length = new FileInfo(file).Length;

        using var db = PagedDb.MemoryMappedDb((ulong)length, settings.HistoryDepth, settings.Path);

        // configure merkle to memoize none and recalculate all
        const int none = int.MaxValue;
        var merkle = new ComputeMerkleBehavior(none, none, false);

        await using var blockchain = new Blockchain(db, merkle);
        using var batch = blockchain.StartReadOnlyLatestFromDb();

        AnsiConsole.MarkupLine("[grey]Calculation in progress...[/]");

        var sw = Stopwatch.StartNew();
        var hash = merkle.CalculateStateRootHash(batch);
        var took = sw.Elapsed;

        AnsiConsole.MarkupLine($"[grey]Calculation took: {took}[/]");

        if (hash == batch.Hash)
        {
            AnsiConsole.MarkupLine($"[green]Recalculated hash matches the expected {hash.ToString(true)}[/]");
            return 0;
        }

        AnsiConsole.MarkupLine($"[red] Recalculated hash {hash.ToString(true)} " +
                                   $"is different than one memoized {batch.Hash.ToString(true)}[/]");

        return -1;
    }
}