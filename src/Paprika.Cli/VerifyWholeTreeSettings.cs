using Paprika.Chain;
using Paprika.Merkle;
using Spectre.Console;
using Spectre.Console.Cli;

namespace Paprika.Cli;

public class VerifyWholeTreeSettings : BasePaprikaSettings
{
    public class Command : Command<VerifyWholeTreeSettings>
    {
        public override int Execute(CommandContext context, VerifyWholeTreeSettings settings)
        {
            using var db = settings.BuildDb();

            using var read = db.BeginReadOnlyBatch();
            using var latest = Blockchain.StartReadOnlyLatestFromDb(db);

            AnsiConsole.WriteLine("Checking whole tree...");

            var keccak = new ComputeMerkleBehavior().CalculateStateRootHash(latest);

            AnsiConsole.WriteLine($"Keccak {keccak.ToString()}");


            return 0;
        }
    }
}