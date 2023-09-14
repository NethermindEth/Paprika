// See https://aka.ms/new-console-template for more information

using Nethermind.Core.Crypto;
using Nethermind.Db;
using Nethermind.Db.Rocks;
using Nethermind.Db.Rocks.Config;
using Nethermind.Logging;
using Nethermind.Serialization.Rlp;
using Nethermind.State;
using Nethermind.Trie.Pruning;

Console.WriteLine("Hello, World!");

// StandardDbInitializer
// MemoryHintMan
// public int? StateDbBlockSize { get; set; } = 4 * 1024;

const string path = @"C:\Users\Szymon\ethereum\execution\nethermind_db\sepolia";
var logs = LimboLogs.Instance;
var cfg = DbConfig.Default;

using var state = new DbOnTheRocks(path, new RocksDbSettings(DbNames.State, DbNames.State), cfg, logs);
using var blockInfos = new DbOnTheRocks(path, new RocksDbSettings(DbNames.BlockInfos, DbNames.BlockInfos), cfg, logs);
using var headers = new DbOnTheRocks(path, new RocksDbSettings(DbNames.Headers, DbNames.Headers), cfg, logs);
using var store = new TrieStore(state, logs);

var headAddressInDb = Keccak.Zero;
var headData = blockInfos.Get(headAddressInDb);
ArgumentNullException.ThrowIfNull(headData, "Block info head not found");

var head = new Keccak(headData);
var headerData = headers.Get(head);
ArgumentNullException.ThrowIfNull(headerData, "Header of the head");

var decoder = new HeaderDecoder();
var headHeader = decoder.Decode(new RlpStream(headerData));
ArgumentNullException.ThrowIfNull(headHeader, "Head has no header!");
ArgumentNullException.ThrowIfNull(headHeader.StateRoot, "Head header! has no StateRoot!");

var trie = new StateTree(store, logs)
{
    RootHash = headHeader.StateRoot
};

trie.RootRef!.ResolveNode(store);

Console.WriteLine($"Root: {trie.RootHash}" );
