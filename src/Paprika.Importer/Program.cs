// See https://aka.ms/new-console-template for more information

using System.Diagnostics;
using Nethermind.Blockchain.Headers;
using Nethermind.Core;
using Nethermind.Core.Crypto;
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
const string path = @"C:\Users\Szymon\ethereum\execution\nethermind_db\sepolia";

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

var bestPersisted = blockInfos.Get(bestPersistedState.Value);
var chainLevel = Rlp.GetStreamDecoder<ChainLevelInfo>()!.Decode(new RlpStream(bestPersisted!));
var main = chainLevel.BlockInfos[0];

var header = headerStore.Get(main.BlockHash);

var rootHash = header.StateRoot!;

var trie = new StateTree(store, logs)
{
    RootHash = rootHash
};

//var keccak = new ValueKeccak("0x380c98b03a3f72ee8aa540033b219c0d397dbe2523162db9dd07e6bbb015d50b").ToKeccak();
var nibs = new byte[0];//Nibbles.FromBytes(keccak.Bytes);

var current = trie.RootRef!;

var i = 0;
for (; i < nibs.Length; i++)
{
    current.ResolveNode(store);
    if (current.NodeType == NodeType.Leaf)
        break;
    current = current.GetChild(store, (byte)nibs[i])!;
}

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

using var db = PagedDb.MemoryMappedDb(size, 64, dataPath, false);
//using var db = PagedDb.NativeMemoryDb(size, 2);

const bool skipStorage = false;

// the prefix that should only be scanned
// byte[] nibbles = nibs.AsSpan(0,i).ToArray().Select(n=>(byte)n).ToArray();
byte[] nibbles = Array.Empty<byte>();
var root = MoveDownInTree(nibbles, trie, store);

// var storageCapture = new PaprikaStorageCapturingVisitor();
// root.Accept(storageCapture, store, false, nibbles);
// File.WriteAllText("storage-big-tree.txt",storageCapture.Payload);

using var preCommit = new ComputeMerkleBehavior(true, 1, 1, true);

var rootHashActual = Keccak.Zero;
if (dbExists == false)
{
    await using (var blockchain =
                 new Blockchain(db, preCommit, TimeSpan.FromSeconds(10), 100, () => reporter.Observe()))
    {
        var visitor = new PaprikaCopyingVisitor(blockchain, 5000, skipStorage);
        Console.WriteLine("Starting...");

        var visit = Task.Run(() =>
        {
            root.Accept(visitor, store, false, nibbles);
            visitor.Finish();
        });

        var copy = visitor.Copy();
        await Task.WhenAll(visit, copy);

        rootHashActual = await copy;
    }

    db.ForceFlush();

    using var read = db.BeginReadOnlyBatch("Statistics");
    StatisticsForPagedDb.Report(layout[stats], read);
}
else
{
    await using (var blockchain =
                 new Blockchain(db, preCommit, TimeSpan.FromSeconds(10), 100, () => reporter.Observe()))
    {
        var visitor = new PaprikaAccountValidatingVisitor(blockchain, preCommit, 1000);

        var visit = Task.Run(() =>
        {
            root.Accept(visitor, store, true, nibbles);
            visitor.Finish();
        });

        var validation = visitor.Validate();
        await Task.WhenAll(visit, validation);

        var report = await validation;

        File.WriteAllText("validation-report.txt", report);

        layout[stats].Update(new Panel("validation-report.txt").Header("Paprika accounts different from the original")
            .Expand());
    }
}

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