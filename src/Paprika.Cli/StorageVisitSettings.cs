using Nethermind.Core;
using Paprika.Crypto;
using Paprika.Tests;
using Spectre.Console;
using Spectre.Console.Cli;

namespace Paprika.Cli;

public class StorageVisitSettings : BasePaprikaSettings
{
    [CommandArgument(NextArgPosition, "<address>")]
    public string Address { get; set; }

    public class Command : Command<StorageVisitSettings>
    {
        public override int Execute(CommandContext context, StorageVisitSettings settings)
        {
            using var db = settings.BuildDb();
            using var read = db.BeginReadOnlyBatch();

            var address = new Address(settings.Address);

            var keccak = new Keccak(address.ToAccountPath.Bytes);

            var account = read.GetAccount(keccak);

            AnsiConsole.WriteLine($"{address}: Balance: {account.Balance}, Nonce: {account.Nonce}, {account.StorageRootHash}");

            return 0;
        }
    }
}