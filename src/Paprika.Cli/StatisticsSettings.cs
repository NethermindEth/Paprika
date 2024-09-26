using Paprika.Runner;
using Paprika.Store;
using Spectre.Console;
using Spectre.Console.Cli;

namespace Paprika.Cli;

public class StatisticsSettings : BasePaprikaSettings
{
    public class Command : AsyncCommand<StatisticsSettings>
    {
        public override async Task<int> ExecuteAsync(CommandContext context, StatisticsSettings settings)
        {
            using var db = settings.BuildDb();
            using var read = db.BeginReadOnlyBatch();

            var stats = new Layout("Stats");

            var spectre = new CancellationTokenSource();

            var reportingTask = Task.Run(() => AnsiConsole.Live(stats)
                .StartAsync(async ctx =>
                {
                    while (spectre.IsCancellationRequested == false)
                    {
                        ctx.Refresh();
                        await Task.Delay(500);
                    }

                    // the final report
                    ctx.Refresh();
                }));

            StatisticsForPagedDb.Report(stats, read, db);
            spectre.Cancel();

            await reportingTask;

            return 0;
        }
    }
}