// See https://aka.ms/new-console-template for more information

using System;
using System.Diagnostics;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Nethermind.Blockchain.Headers;
using Nethermind.Core;
using Nethermind.Db;
using Nethermind.Db.Rocks;
using Nethermind.Db.Rocks.Config;
using Nethermind.Logging;
using Nethermind.Serialization.Rlp;
using Nethermind.State;
using Nethermind.Trie;
using Nethermind.Trie.Pruning;
using Paprika.Chain;
using Paprika.Importer;
using Paprika.Merkle;
using Paprika.Runner;
using Paprika.Store;
using Spectre.Console;
using Keccak = Paprika.Crypto.Keccak;

//const string path = @"C:\Users\Szymon\ethereum\mainnet";
string path = args.Length == 0
    ? @"C:\Git\nethermind\src\Nethermind\artifacts\bin\Nethermind.Runner\release\nethermind_db\mainnet"
    : args[0];

//const string path = "~/execution-data/nethermind_db/mainnet/";

var logs = LimboLogs.Instance;
var cfg = DbConfig.Default;

using var state = new DbOnTheRocks(path, GetSettings(DbNames.State), cfg, logs).WithEOACompressed();
using var blockInfos = new DbOnTheRocks(path, GetSettings(DbNames.BlockInfos), cfg, logs);
using var headers = new DbOnTheRocks(path, GetSettings(DbNames.Headers), cfg, logs);
using var blockNumbers = new DbOnTheRocks(path, GetSettings(DbNames.BlockNumbers), cfg, logs);
var headerStore = new HeaderStore(headers, blockNumbers);

using var trieStore = new TrieStore(state, logs);
var store = trieStore.GetTrieStore(null);

// from BlockTree.cs
var stateHeadHashDbEntryAddress = new byte[16];
var persistedNumberData = blockInfos.Get(stateHeadHashDbEntryAddress);
long? bestPersistedState = persistedNumberData is null ? null : new RlpStream(persistedNumberData).DecodeLong();

ArgumentNullException.ThrowIfNull(bestPersistedState, "Best persisted state not found");

var trie = new StateTree(store, logs);

var back = 0;
var blockNumber = bestPersistedState.Value;
TreePath emptyPath;
do
{
    var bestPersisted = blockInfos.Get(blockNumber - back);
    back++;

    var chainLevel = Rlp.GetStreamDecoder<ChainLevelInfo>()!.Decode(new RlpStream(bestPersisted!));
    var main = chainLevel.BlockInfos[0];

    var header = headerStore.Get(main.BlockHash);

    var rootHash = header.StateRoot!;

    trie.RootHash = rootHash;

    if (back > 1000)
    {
        throw new Exception($"Searched for {back} block back since {blockNumber} and failed to load the root");
    }

    emptyPath = TreePath.Empty;
} while (trie.RootRef.TryResolveNode(store, ref emptyPath) == false);

var dir = Directory.GetCurrentDirectory();
var dataPath = Path.Combine(dir, "db");
var dbExists = Directory.Exists(dataPath);

const long GB = 1024 * 1024 * 1024;
var size = (path.Contains("mainnet") ? 320L : 64L) * GB;

if (dbExists)
{
    Console.WriteLine($"DB detected at {dataPath}. Not running import");
    return;
}

Directory.CreateDirectory(dataPath);
Console.WriteLine($"Using persistent DB on disk, located: {dataPath}");
Console.WriteLine("Initializing db of size {0}GB", size / GB);

var layout = new Layout("Metrics");
using var reporter = new MetricsReporter();
layout.Update(reporter.Renderer);

var spectre = new CancellationTokenSource();
var reportingTask = Task.Run(() => AnsiConsole.Live(layout)
    .StartAsync(async ctx =>
    {
        while (spectre.IsCancellationRequested == false)
        {
            reporter.Observe();

            ctx.Refresh();
            await Task.Delay(500);
        }

        // the final report
        reporter.Observe();
        ctx.Refresh();
    }));

var sw = Stopwatch.StartNew();

using var db = PagedDb.MemoryMappedDb(size, 32, dataPath, false);

const bool skipStorage = false;

using var preCommit = new ComputeMerkleBehavior(1);

var rootHashActual = Keccak.Zero;
var budget = new CacheBudget.Options(1_000, 8);
await using (var blockchain =
             new Blockchain(db, preCommit, TimeSpan.FromSeconds(10),
                 budget,
                 budget, 50, int.MaxValue, () => reporter.Observe()))
{
    var visitor = new PaprikaCopyingVisitor(blockchain, 10_000, skipStorage);
    var visit = Task.Run(() =>
    {
        try
        {
            trie.Accept(visitor, trie.RootHash, new VisitingOptions
            {
                ExpectAccounts = true,
                MaxDegreeOfParallelism = 8,
                //FullScanMemoryBudget = 1L * 1024 * 1024 * 1024
            });

            visitor.Finish();
        }
        catch
        {
            spectre.Cancel();
            throw;
        }
    });

    var copy = visitor.Copy();
    await Task.WhenAll(visit, copy);

    rootHashActual = await copy;
}

db.ForceFlush();
spectre.Cancel();
await reportingTask;

AnsiConsole.WriteLine(
        $"Root: {trie.RootHash} was being imported to Paprika in {sw.Elapsed:g} and resulted in {rootHashActual}");

return;

DbSettings GetSettings(string dbName)
{
    var dbPath = dbName;

    if (dbName == DbNames.State)
    {
        // pruned database, append version 
        dbPath = Path.Combine(dbName, "0");
        dbName += "0";
    }

    return new DbSettings(dbName, dbPath);
}