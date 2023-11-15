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

var keccak = new ValueKeccak("0x380c98b03a3f72ee8aa540033b219c0d397dbe2523162db9dd07e6bbb015d50b").ToKeccak();
var nibs = Nibbles.FromBytes(keccak.Bytes);

var current = trie.RootRef!;

var i = 0;
for (; i < nibs.Length; i++)
{
    current.ResolveNode(store);
    if (current.NodeType == NodeType.Leaf)
        break;
    current = current.GetChild(store, (byte)nibs[i])!;
}

//var bytes = trie.Get(keccak.Bytes);
//var decoded = new AccountDecoder().Decode(new RlpStream(bytes!));
// var visiting = new StorageTree(store, logs) { RootHash = decoded!.StorageRoot };

//
// var wrongStorageRoots = @"0x05b5f4b7b34880c3599decc29996d7d171f9551fb8149e74accf3d9de80b5e93,
// 0x07f28a66f5ada6a7db725adad6f6572b9f1ca21efef0ffe4134d144a9d642d2c,
// 0x08453b9842c049474c103b3dda8b0a53a99a670d28a2b9f18dd57c4a7220a21d,
// 0x09a6dbc028df942f095cad6efa1c8e196a00cc2b59e9d555a66b1329704b7065,
// 0x0e3b6f07b5c21f08d02ce4fb6e4da3f3e33f7e324f3b1ebd12c22a3fdca4e7f2,
// 0x0e82cb48c1b6771bb99a7b1c9b0e0d82d73f8181c2bf3355206edbd255036f8f,
// 0x0ed6955a118d92ef74fba6ce17b8eb6c097a9d14b372039f5fd5a13260af0211,
// 0x10d4ea4b1a61bf47c9da92a23f051aae8424813a2d486778d30d40721b46b460,
// 0x123f112af635e72fa27d0b4724cf234e54544f270a165fa3dc5d7dd032187b10,
// 0x1d0dd4016e8b63360d8bcd376eb61bf80f4e2e872ed064a0eac1ff06dd24db81,
// 0x228cf8cc9dcf991e16d2613aa83677dd678337416321b994bb1b5deed6f48692,
// 0x23337b54a9f65b7fd3fe46b5f8320d0de814769d6614f21f286a12f091fa9b52,
// 0x24a7de3085e4270fa5f2e89174764973d5f850b8028a06c0a41f2d3a8ff51981,
// 0x24b77863a66ad81dd3c36b7a7d474d5e00a9a3134b485d6d2064db3878337f2e,
// 0x293cba052597c16e13f735857cd3fa3702211b77eedbf18629f25ec290c69ed2,
// 0x380c98b03a3f72ee8aa540033b219c0d397dbe2523162db9dd07e6bbb015d50b,
// 0x3cd36ff59c01d0462cceaae6e1035671e402e3ece75d6856601a800b307c4610,
// 0x40a07cca6e0b53665107ef820d464f75e32557c1cc15453ddee10947b387bd8e,
// 0x487a601686c96e5609c955becb2d08daa06750dbdd94d0266db775711c8df11a,
// 0x4d15e348c820f268c6f7f471f700331b1d5211ae9148188df0135fcb476d9f90,
// 0x4d6358a4f291df91c256051466c94207e960b59e989f8f7b97453e8ae2b85866,
// 0x4f2e2133870160e17c85a7e1eca7bd2f27c6f14f45d31f9eb75ca2c4dea53a5c,
// 0x4fc3b9e5f29d3e51949ff63bc8a4be28c9356437099cb15efc90db25bb370c55,
// 0x5363cf3d10cae80747e18e719e4c819fbdd8ac72bc1fea4a77061970661fb636,
// 0x54ab3ac6404714a0d4496595444787abc75a5e1c0ec73932799928f752a2448b,
// 0x5c6b2ee183961e66841d9fcd18b899395e596cff702b6f8b26134a88f4b4b3e5,
// 0x5d7f00bd44756ab4e9fd72201091fbfc943e057e63b01d52b9a3a9f1ae45a0d8,
// 0x60b48482120a358e1d613bf014638c7ffa81f2bf2c3927b232ead52fb390e72f,
// 0x646373692a7de1ea9a5e258b787c8e0715aaefb816c00ecad73b836b69c64290,
// 0x64e4a2b26c516f57f392b5d7ce915be86e08f68a8f553d54389965ebbec3919d,
// 0x66938628ffd4c0cdae95847ec3744e752d62ec782589c079f8020a2f64972182,
// 0x66d778f608b09468d014826ec8594c05fca4b8790320e9a26a80156f53ebfe75,
// 0x6732c06025245dcb5348b5ff2e0b5b574a391ef06cdd5962725eecd402061331,
// 0x6bd6974a112309d3b4ec54c07f62f9081d011b3df9c075a6d5f8305dee41729c,
// 0x6ec52cfb4316b1b9351c75fe1c3a1f5df5c03686f6a742bffd11b809dc56019f,
// 0x7c21666ee0b0179dad0da029b1c7a6e70a17000ba2213c9c627d064bf32407d7,
// 0x7f2417f398e7ddc0116c16304a25efd3c093a8dd733151dfe8b966da4506677e,
// 0x862c7ec27d055f9f0e2deb2da4f3f057fc6a374887518807a443c0dcef9c6343,
// 0x8e0401707d3f041e770dff90409235e62e9e26b29b9eef6bb0a6158defa9f778,
// 0x94d0c91fd24487863758bd3fd91a545a55fa57e7e04b071ce2be638da250ce9c,
// 0x9530d4559172fcae2b684926ee2f5059865a7a2fc4c54bd0cdbd8764bc6ea7cb,
// 0x9569fdb2843ac771f55e0ba27291d562bc9bb7044b1e25e2ca905d602db56d1f,
// 0xa1ab4b79b27189719d4097a976ed4184eb97283f238f8fdb0909e0f602036bc6,
// 0xa2cabe7f385175bef10ddab368bf2aa23c52e464c0cbe27e74c9018736175bba,
// 0xa83375f5cdd4659df7a7469b26c0e399d384d3bdccb9b1ffa68a114b0a5ac6ca,
// 0xa84132379e18f00dfbcf7d8396673cbe8d46b8d87a830147208944fc15f47fb2,
// 0xa9a573117aab1a80c904fe15d697acab3e3906af3a2f1495395b2e9a401aba6f,
// 0xac6fd2488766fa072d492404399774e17e374b9b69fe7633027d91c6a64e8165,
// 0xb09c7ada69bf6e2f4e32057e095d06d0939963988ef987d49c508d20137661fa,
// 0xb120cc71ce40058015f7e2fc7cdb6ac94130b3cb3fa7ed6bd6edd80489889686,
// 0xb21947d14cf70932b99ae984f13859589c5c90f7c2c1abcad72c99a4de6d4a53,
// 0xb907c0d8af3816608ef655d1799f6b688e89fe74d3a77dab2ed347c5c30f5c45,
// 0xbbc370c06a145efbb67ed2ae9fe75fe858175c5153e03fbe5192636378c385c6,
// 0xbef6b29b5b0853513c5977f84b71dd8958252e8f993970c4b39763897c15a9ef,
// 0xc53dcb3cec37c26f217a47290f64bd1e7b039cda9588714cdeb04ae574afb3ef,
// 0xc5db61ab9e5b9231fc39796dade86b62ffc424029edc0bc865644a6e2fbe7d67,
// 0xc76403d5d7db3dabc5df758991b605dfa2f2548087b3aea21263530f8de19088,
// 0xc764f5d7ace7a9cf44e1650b0a11bc663395ef942689de3b316399af73a9c591,
// 0xce233e2bc301f8d72d8f74fd73bddb2039cecd75e3a3de3d12d6f8c1afbfbbdc,
// 0xcf881d2b30c72e16cc1953eee731fdf5297f88e9ef0f7bb6bf79c0432045521f,
// 0xcff4657c40e9b79f0b92061a7f7af83bfde14713c82d86d1e1a667a4c0e061df,
// 0xd08ad7b84cddeca3cb0e363cbcaa69c89b6d158290ef18092bee0c0ad043da7c,
// 0xd39b30a98fa42a96ee72d5766faf57d3e36b110f5ed3be5d8e384ee3aff11903,
// 0xd3ff0a78a6199e3da04616f9be71eafc12c28ff90580c4299b2f74f13682081d,
// 0xd73a738be67d44909f3d1f4dad02896babf7135b9af0a14a5ed37d9dded2e761,
// 0xdc9fc1ebaa2d03729294c2f5dae3f372851c7a20b587f5cc6e7c8a2f765c0a07,
// 0xdd2cf6a225ee9ad520b1d9e463259b864c51657d3cb4e5a2269c351a03538049,
// 0xdf355d2b309cf399c1147331b3dfb3c0fdad29a7ee06922e030e91f330844a4f,
// 0xdf4f931ece37985073663daefe3ba6bc4df5a73bd1a309b638210ecc43f7c545,
// 0xdfbf81d032aa70948f95d26037005d002915adc95ea3bd28f13a36896cb129ac,
// 0xe6720bf2a7466145ae52c7e391e325bd6f7d49481bde13a0f43437f305e8426e,
// 0xe9d2779159d4e12941ce7e449707540bb2dda66df39696774c2947fad1bda9f7,
// 0xecfe07587c9b7c8aff7af5bc9121ff514d96b48652c4cb3cc44d3b90e034a3ce,
// 0xf0a8468af0ac38a147d4b409cf677a7a196b575582e970910ca0669d282ff9a3,
// 0xf34b9941ee116f6ca28401704d4c7ee0000ae11fba47d9d2c890e4b42653feac,
// 0xf498577029c9548d6009b6c5c03f938b27c68e9678c7f41ed1bf4befc04ae261,
// 0xfbe66300c7ac22fe3439cf2215bf0f5acffb412aca10080b0599db9440c16e43,
// 0xfc163ba969f504b4370cfacdaa5f84ca91e9f4b7992878fcd4a07d47df9ba1f2";
//
// var keccaks = wrongStorageRoots.Split(",").Select(v => new ValueKeccak(v.Trim()).ToKeccak()).ToArray();
// var statsDict = new List<(Nethermind.Core.Crypto.Keccak, TrieStatsCollector)>();
//
// foreach (var k in keccaks)
// {
//     var bytes = trie.Get(k.Bytes);
//     var decoded = new AccountDecoder().Decode(new RlpStream(bytes!));
//
//     var visiting = new StorageTree(store, logs)
//     {
//         RootHash = decoded!.StorageRoot
//     };
//
//     var trieStats = new TrieStatsCollector(new MemDb(), LimboLogs.Instance);
//     visiting.RootRef.Accept(trieStats, store, new TrieVisitContext { IsStorage = true, MaxDegreeOfParallelism = 0 });
//     statsDict.Add((k, trieStats));
// }
//
// statsDict.Sort((a, b) => a.Item2.Stats.NodesCount.CompareTo(b.Item2.Stats.NodesCount));

var dir = Directory.GetCurrentDirectory();
var dataPath = Path.Combine(dir, "db");
var dbExists = Directory.Exists(dataPath);

const long GB = 1024 * 1024 * 1024;
var size = (path.Contains("mainnet") ? 256ul : 24ul) * GB;

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
byte[] nibbles = nibs.AsSpan(0,i).ToArray().Select(n=>(byte)n).ToArray();
//byte[] nibbles = new byte[] { 0x2 };
var root = MoveDownInTree(nibbles, trie, store);

// var storageCapture = new PaprikaStorageCapturingVisitor();
// root.Accept(storageCapture, store, false, nibbles);
// File.WriteAllText("storage-big-tree.txt",storageCapture.Payload);

using var preCommit = new ComputeMerkleBehavior(true, 2, 2, true);

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