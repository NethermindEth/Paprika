// See https://aka.ms/new-console-template for more information

using System.Diagnostics;
using Nethermind.Core;
using Nethermind.Core.Crypto;
using Nethermind.Db;
using Nethermind.Db.Rocks;
using Nethermind.Db.Rocks.Config;
using Nethermind.Logging;
using Nethermind.Serialization.Rlp;
using Nethermind.State;
using Nethermind.Trie.Pruning;
using Paprika.Importer;

// StandardDbInitializer
// MemoryHintMan
// public int? StateDbBlockSize { get; set; } = 4 * 1024;

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

var chaininfo = blockInfos.Get(bestPersistedState.Value);
var chainLevel = Rlp.GetStreamDecoder<ChainLevelInfo>()!.Decode(new RlpStream(chaininfo!));

var main = chainLevel.BlockInfos[0];
var header = headers.Get(main.BlockHash);

var headerDecoded = Rlp.GetStreamDecoder<BlockHeader>()!.Decode(new RlpStream(header!));

var rootHash = headerDecoded.StateRoot!;

var trie = new StateTree(store, logs)
{
    RootHash = rootHash
};

var visitor = new PaprikaCopyingVisitor();

Console.WriteLine("Starting visitor...");

var sw = Stopwatch.StartNew();

var run = Task.Run(() => trie.Accept(visitor, rootHash, true));

while (true)
{
    var completed = await Task.WhenAny(Task.Delay(1_000), run);
    if (completed == run)
        break;
    Console.WriteLine($"Read accounts: {visitor.Accounts}");
}

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
