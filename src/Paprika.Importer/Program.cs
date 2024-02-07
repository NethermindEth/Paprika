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

using var store = new TrieStore(state, logs);

// from BlockTree.cs
var stateHeadHashDbEntryAddress = new byte[16];
var persistedNumberData = blockInfos.Get(stateHeadHashDbEntryAddress);
long? bestPersistedState = persistedNumberData is null ? null : new RlpStream(persistedNumberData).DecodeLong();

ArgumentNullException.ThrowIfNull(bestPersistedState, "Best persisted state not found");

var trie = new StateTree(store, logs);

var back = 0;
var blockNumber = bestPersistedState.Value;

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
} while (trie.RootRef.TryResolveNode(store) == false);

var dir = Directory.GetCurrentDirectory();
var dataPath = Path.Combine(dir, "db");
var dbExists = Directory.Exists(dataPath);

const long GB = 1024 * 1024 * 1024;
var size = (path.Contains("mainnet") ? 256L : 64L) * GB;

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

//using var db = new Db(dataPath, 64, size, sync: false);
using var db = PagedDb.MemoryMappedDb(size, 64, dataPath, false);

const bool skipStorage = false;

// var storageCapture = new PaprikaStorageCapturingVisitor();
// root.Accept(storageCapture, store, false, nibbles);
// File.WriteAllText("storage-big-tree.txt",storageCapture.Payload);

using var preCommit = new ComputeMerkleBehavior(1, 1);

var rootHashActual = Keccak.Zero;
if (dbExists == false)
{
    await using (var blockchain =
                 new Blockchain(db, preCommit, TimeSpan.FromSeconds(10), new CacheBudget.Options(1_000, 8), 50, () => reporter.Observe()))
    {
        var visitor = new PaprikaCopyingVisitor(blockchain, 50000, skipStorage);
        Console.WriteLine("Starting...");

        var visit = Task.Run(() =>
        {
            trie.Accept(visitor, trie.RootHash, new VisitingOptions
            {
                ExpectAccounts = true,
                MaxDegreeOfParallelism = 6,
                //FullScanMemoryBudget = 4L * 1024 * 1024 * 1024
            });

            visitor.Finish();
        });

        var copy = visitor.Copy();
        await Task.WhenAll(visit, copy);

        rootHashActual = await copy;
    }

    db.ForceFlush();

    // LMDB
    // db.ForceSync();
}
else
{
    using var read = db.BeginReadOnlyBatch();
    StatisticsForPagedDb.Report(layout[stats], read);
    // await using (var blockchain =
    //              new Blockchain(db, preCommit, TimeSpan.FromSeconds(10), CacheBudget.Options.None, 100, () => reporter.Observe()))
    // {
    //     
    //     var visitor = new PaprikaAccountValidatingVisitor(blockchain, preCommit, 1000);
    //
    //     var visit = Task.Run(() =>
    //     {
    //         root.Accept(visitor, store, true, nibbles);
    //         visitor.Finish();
    //     });
    //
    //     var validation = visitor.Validate();
    //     await Task.WhenAll(visit, validation);
    //
    //     var report = await validation;
    //
    //     File.WriteAllText("validation-report.txt", report);
    //
    //     layout[stats].Update(new Panel("validation-report.txt").Header("Paprika accounts different from the original")
    //         .Expand());
    // }

    // LMDB
    // var statistics = db.GatherStats();
    // layout[stats].Update(new Panel(statistics.ToString()).Header("LMDB stats"));
}

spectre.Cancel();
await reportingTask;

if (dbExists == false)
{
    Console.WriteLine($"Root: {trie.RootHash} was being imported to Paprika in {sw.Elapsed:g} and resulted in {rootHashActual}");
}

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

static TrieNode MoveDownInTree(byte[] nibbles, PatriciaTree trie, ITrieNodeResolver store)
{
    var root = trie.RootRef!;

    for (var i = 0; i < nibbles.Length; i++)
    {
        root.ResolveNode(store, ReadFlags.HintCacheMiss);
        root = root.GetChild(store, nibbles[i])!;
    }

    root.ResolveNode(store, ReadFlags.HintCacheMiss);

    return root;
}