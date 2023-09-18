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
using Paprika.Importer;
using Paprika.Merkle;
using Paprika.Store;

const string path = @"C:\Users\Szymon\ethereum\execution\nethermind_db\sepolia";
var logs = LimboLogs.Instance;
var cfg = DbConfig.Default;

using var state = new DbOnTheRocks(path, GetSettings(DbNames.State), cfg, logs).WithEOACompressed();
using var blockInfos = new DbOnTheRocks(path, GetSettings(DbNames.BlockInfos), cfg, logs);
using var headers = new DbOnTheRocks(path, GetSettings(DbNames.Headers), cfg, logs);
using var store = new TrieStore(state, logs);

// from BlockTree.cs
byte[] stateHeadHashDbEntryAddress = new byte[16];
var persistedNumberData = blockInfos.Get(stateHeadHashDbEntryAddress);
long? bestPersistedState = persistedNumberData is null ? null : new RlpStream(persistedNumberData).DecodeLong();

ArgumentNullException.ThrowIfNull(bestPersistedState, "Best persisted state not found");

var info = blockInfos.Get(bestPersistedState.Value);
var chainLevel = Rlp.GetStreamDecoder<ChainLevelInfo>()!.Decode(new RlpStream(info!));

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

if (Directory.Exists(dataPath))
{
    Console.WriteLine("Deleting previous db...");
    Directory.Delete(dataPath, true);
}

const long GB = 1024 * 1024 * 1024;
const long size = 16 * GB;

Directory.CreateDirectory(dataPath);
Console.WriteLine($"Using persistent DB on disk, located: {dataPath}");
Console.WriteLine("Initializing db of size {0}GB", size / GB);

using var db = PagedDb.MemoryMappedDb(size, 2, dataPath, false);

using var preCommit = new ComputeMerkleBehavior(true, 2, 1);
await using var blockchain = new Blockchain(db, preCommit, TimeSpan.FromSeconds(10), 1000);

var visitor = new PaprikaCopyingVisitor(blockchain, 1_000);

Console.WriteLine("Starting...");

var sw = Stopwatch.StartNew();
var copy = Task.Run(() => visitor.Copy());

var run = Task.Run(() => trie.Accept(visitor, rootHash, true));

// expected count
const int count = 16146399;

while (true)
{
    var completed = await Task.WhenAny(Task.Delay(1_000), run);
    if (completed == run)
        break;

    var msg = count > 0 ? $"{(double)visitor.Accounts / count:P2}" : "";
    Console.WriteLine($"Read accounts: {visitor.Accounts,16} {msg}");
}

visitor.Finish();

Console.WriteLine("Awaiting writes to finish...");
await copy;

Console.WriteLine($"Root: {rootHash} had {visitor.Accounts} accounts in {sw.Elapsed:g}");

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