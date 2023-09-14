// See https://aka.ms/new-console-template for more information

using Nethermind.Core.Crypto;
using Nethermind.Db;
using Nethermind.Db.Rocks;
using Nethermind.Db.Rocks.Config;
using Nethermind.Logging;
using Nethermind.State;
using Nethermind.Trie.Pruning;

Console.WriteLine("Hello, World!");

var settings = new RocksDbSettings(DbNames.State, DbNames.State);

using var state = new DbOnTheRocks(@"C:\Users\Szymon\ethereum\execution\nethermind_db\sepolia", settings, DbConfig.Default, LimboLogs.Instance);
using var store = new TrieStore(state, LimboLogs.Instance);
var trie = new StateTree(store, LimboLogs.Instance);

// https://sepolia.etherscan.io/block/0x77f53fbcafe5d70031154923852b234daa9acddcb7b3212b936b19116d93c15b
var rootHash = new Keccak("0x4f32764e59eccac237bc6fc78b24f97ad17336906209ab537eedea3240567e7d");

trie.RootHash = rootHash;

Console.WriteLine($"Root: {trie.RootHash}" );
