// See https://aka.ms/new-console-template for more information

using System.Diagnostics;
using Nethermind.Core;
using Nethermind.Db;
using Nethermind.Db.Rocks;
using Nethermind.Db.Rocks.Config;
using Nethermind.Logging;
using Nethermind.Serialization.Rlp;
using Nethermind.State;
using Nethermind.Trie.Pruning;
using Paprika.Chain;
using Paprika.Crypto;
using Paprika.Importer;
using Paprika.Merkle;
using Paprika.Runner;
using Paprika.Store;
using Spectre.Console;

//const string path = @"C:\Users\Szymon\ethereum\mainnet";
const string path = @"C:\Users\Szymon\ethereum\execution\nethermind_db\sepolia";

var logs = LimboLogs.Instance;
var cfg = DbConfig.Default;

using var state = new DbOnTheRocks(path, GetSettings(DbNames.State), cfg, logs).WithEOACompressed();
using var blockInfos = new DbOnTheRocks(path, GetSettings(DbNames.BlockInfos), cfg, logs);
using var headers = new DbOnTheRocks(path, GetSettings(DbNames.Headers), cfg, logs);
using var store = new TrieStore(state, logs);

// from BlockTree.cs
var stateHeadHashDbEntryAddress = new byte[16];
var persistedNumberData = blockInfos.Get(stateHeadHashDbEntryAddress);
long? bestPersistedState = persistedNumberData is null ? null : new RlpStream(persistedNumberData).DecodeLong();

ArgumentNullException.ThrowIfNull(bestPersistedState, "Best persisted state not found");

var bestPersisted = blockInfos.Get(bestPersistedState.Value);
var chainLevel = Rlp.GetStreamDecoder<ChainLevelInfo>()!.Decode(new RlpStream(bestPersisted!));

var main = chainLevel.BlockInfos[0];

var header = headers.Get(main.BlockHash);
var headerDecoded = Rlp.GetStreamDecoder<BlockHeader>()!.Decode(new RlpStream(header!));

var rootHash = headerDecoded.StateRoot!;

var trie = new StateTree(store, logs)
{
    RootHash = rootHash
};

var dir = Directory.GetCurrentDirectory();
var dataPath = Path.Combine(dir, "db");
var dbExists = Directory.Exists(dataPath);

const long GB = 1024 * 1024 * 1024;
var size = (path.Contains("mainnet") ? 256ul : 32ul) * GB;

if (dbExists)
{
    Console.WriteLine($"DB detected at {dataPath}. Will run just statistics...");
}
else
{
    Directory.CreateDirectory(dataPath);
    Console.WriteLine($"Using persistent DB on disk, located: {dataPath}");
    Console.WriteLine("Initializing db of size {0}GB", size / GB);
}

// reporting
const string metrics = "Metrics";
const string stats = "Stats";

var layout = new Layout("Runner")
    .SplitRows(new Layout(metrics), new Layout(stats));

using var reporter = new MetricsReporter();
layout[metrics].Update(reporter.Renderer);
layout[stats].Update(new Panel("Statistics will appear here after the import finishes").Header("Statistics").Expand());

var spectre = new CancellationTokenSource();
var reportingTask = Task.Run(() => AnsiConsole.Live(dbExists ? layout.GetLayout(stats) : layout)
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

//using var db = PagedDb.MemoryMappedDb(size, 64, dataPath, false);
using var db = PagedDb.NativeMemoryDb(size, 2);

var rootHashActual = Keccak.Zero;
if (dbExists == false)
{
    using var preCommit = new ComputeMerkleBehavior(true, 2, 2, true);

    await using (var blockchain =
                 new Blockchain(db, preCommit, TimeSpan.FromSeconds(10), 100, () => reporter.Observe()))
    {
        var visitor = new PaprikaRootValidatingVisitor(blockchain, 200);
        Console.WriteLine("Starting...");

        var visit = Task.Run(() =>
        {
            trie.Accept(visitor, rootHash, true);
            visitor.Finish();
        });

        var copy = visitor.Copy();
        await Task.WhenAll(visit, copy);

        rootHashActual = await copy;
    }

    db.ForceFlush();
}

using var read = db.BeginReadOnlyBatch("Statistics");

StatisticsForPagedDb.Report(layout[stats], read);

spectre.Cancel();
await reportingTask;

if (dbExists == false)
{
    Console.WriteLine($"Root: {rootHash} was being imported to Paprika in {sw.Elapsed:g} and resulted in {rootHashActual}");
}

return;

RocksDbSettings GetSettings(string dbName)
{
    var dbPath = dbName;

    if (dbName == DbNames.State)
    {
        // pruned database, append version 
        dbPath = Path.Combine(dbName, "0");
        dbName += "0";
    }

    return new RocksDbSettings(dbName, dbPath);
}