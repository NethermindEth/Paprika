using Paprika.Runner;
using Paprika.Store;
using Spectre.Console;
using Spectre.Console.Cli;

namespace Paprika.Cli;

public class StatisticsSettings : BasePaprikaSettings
{
    public class Command : Command<StatisticsSettings>
    {
        public override int Execute(CommandContext context, StatisticsSettings settings)
        {
            using var db = settings.BuildDb();
            using var read = db.BeginReadOnlyBatch();

            var stats = new Layout("Stats");

            AnsiConsole.WriteLine("Gathering stats...");

            StatisticsForPagedDb.Report(stats, read, db);

            AnsiConsole.Write(stats);

            return 0;
        }
    }
}