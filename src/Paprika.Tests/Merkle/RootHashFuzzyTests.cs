using System.Buffers.Binary;
using System.Reflection;
using FluentAssertions;
using NUnit.Framework;
using Paprika.Chain;
using Paprika.Crypto;
using Paprika.Data;
using Paprika.Merkle;
using Paprika.Store;
using Paprika.Tests.Store;
using Spectre.Console;

namespace Paprika.Tests.Merkle;

public class RootHashFuzzyTests
{
    [TestCase(nameof(Accounts_1000))]
    [TestCase(nameof(Accounts_10_000))]
    //[TestCase(nameof(Accounts_1_000_000))]
    //[TestCase(nameof(Accounts_10_000_000))]
    public void Compute_Twice(string test)
    {
        var generator = Build(test);

        var commit = new Commit();

        generator.Run(commit);

        // assert twice to ensure that the root is not changed
        AssertRoot(generator.RootHash, commit);

        AssertRoot(generator.RootHash, commit);
    }

    [TestCase(nameof(Accounts_100_Storage_1), int.MaxValue, 4)]
    [TestCase(nameof(Accounts_1_Storage_100), 11, 8)]
    [TestCase(nameof(Accounts_100_Storage_100), int.MaxValue, 756, Category = Categories.LongRunning)]
    public async Task In_memory_run(string test, int commitEvery, int blockchainPoolSizeMB)
    {
        var generator = Build(test);

        using var db = PagedDb.NativeMemoryDb(32 * 1024 * 1024, 2);
        var merkle = new ComputeMerkleBehavior();
        await using var blockchain = new Blockchain(db, merkle);

        var rootHash = await generator.Run(blockchain, commitEvery);
        AssertRootHash(rootHash, generator);

        AssertBlockchainMaxPoolSize(blockchain, blockchainPoolSizeMB);
    }

    private static void AssertBlockchainMaxPoolSize(Blockchain blockchain, int expected)
    {
        blockchain.PoolAllocatedMB.Should().BeLessOrEqualTo(expected,
            "Upper boundary set by running this test. Bigger number means too much memory allocated.");
    }

    [Test]
    public async Task CalculateStateRootHash(
        [Values(
            nameof(Accounts_1_Storage_100), nameof(Accounts_100_Storage_1),
            nameof(Accounts_1000_Storage_1))]
        string test,
        [Values(int.MaxValue, 23)] int commitEvery,
        [Values(true, false)] bool parallel)
    {
        var generator = Build(test);

        using var db = PagedDb.NativeMemoryDb(32 * 1024 * 1024, 2);
        var parallelism = parallel ? ComputeMerkleBehavior.ParallelismUnlimited : ComputeMerkleBehavior.ParallelismNone;
        var merkle = new ComputeMerkleBehavior(parallelism);

        await using var blockchain = new Blockchain(db, merkle);

        var rootHash = await generator.Run(blockchain, commitEvery, false);

        await blockchain.Finalize(rootHash);

        using var read = db.BeginReadOnlyBatch();
        read.VerifyNoPagesMissing();

        using var state = blockchain.StartReadOnly(rootHash);
        var recalculated = merkle.CalculateStateRootHash(state);

        rootHash.Should().Be(generator.RootHashAsKeccak);
        recalculated.Should().Be(rootHash);

        // var visitor = new ValueSquashingDictionaryVisitor(db);
        // db.VisitRoot(visitor);
        //
        // var pairs = visitor.Dictionary
        //     .Select(kvp =>
        //     {
        //         NibblePath.ReadFrom(kvp.Key, out var path);
        //         return new ValueTuple<string, string>(path.ToString(), kvp.Value.AsSpan().ToHexString(true));
        //     })
        //     .ToArray();
        //
        // Array.Sort(pairs, (a, b) => a.Item1.CompareTo(b.Item1));
        //
        // foreach (var (key, value) in pairs)
        // {
        //     Console.WriteLine($"{key}: {value}");
        // }

        //AnsiConsole.Write(visitor.Tree);
    }

    [TestCase(nameof(Accounts_10_000), 256 * 1024 * 1024L, 8)]
    [TestCase(nameof(Accounts_1_000_000), 2 * 1024 * 1024 * 1024L, 28, Category = Categories.LongRunning)]
    public async Task CalculateThenDelete(string test, long size, int blockchainPoolSizeMB)
    {
        var generator = Build(test);

        using var db = PagedDb.NativeMemoryDb(size, 2);
        using var merkle = new ComputeMerkleBehavior(ComputeMerkleBehavior.ParallelismNone);
        await using var blockchain = new Blockchain(db, merkle, null, new CacheBudget.Options(2000, 4),
            new CacheBudget.Options(2000, 4));

        // blockchain.VerifyDbIntegrityOnCommit();

        // set
        await generator.Run(blockchain, 513, false, true);

        // delete
        var rootHash = await generator.Run(blockchain, 1001, true, true);

        rootHash.Should().BeOneOf(Keccak.EmptyTreeHash, Keccak.Zero);

        AssertBlockchainMaxPoolSize(blockchain, blockchainPoolSizeMB);
    }

    private static void AssertRootHash(Keccak rootHash, CaseGenerator generator)
    {
        rootHash.Should().Be(generator.RootHashAsKeccak, "Root hashes should match");
    }

    private static CaseGenerator Build(string name) => (CaseGenerator)typeof(RootHashFuzzyTests)
        .GetMethods(BindingFlags.Static | BindingFlags.Public | BindingFlags.NonPublic).First(m => m.Name == name)
        .Invoke(null, null) ?? throw new NullReferenceException("Cannot find!");

    private static CaseGenerator Accounts_1000() =>
        new(1000, 0, "b255eb6261dc19f0639d13624e384b265759d2e4171c0eb9487e82d2897729f0");

    private static CaseGenerator Accounts_10_000() =>
        new(10000, 0, "48864c880bd7610f9bad9aff765844db83c17cab764f5444b43c0076f6cf6c03");

    private static CaseGenerator Accounts_1_000_000() =>
        new(1_000_000, 0, "e46e17a7ffa62ba32679893e6ccb4d9e48a9b044a88f22ff02004e6cc7f005b8");

    private static CaseGenerator Accounts_10_000_000() =>
        new(10_000_000, 0, "a52d8ca37ed3310fa024563ad432df953fabb2130523f78adb1830bda9beccbe");

    private static CaseGenerator Accounts_1_Storage_1() =>
        new(1, 1, "954f21233681f1b941ef67b30c85b64bfb009452b7f01b28de28eb4c1d2ca258");

    private static CaseGenerator Accounts_1_Storage_100() =>
        new(1, 100, "c8cf5e6b84e39beeac713a42546cc977581d9b31307efa2b1b288ccd828f278e");

    private static CaseGenerator Accounts_100_Storage_1() =>
        new(100, 1, "68965a86aec45d3863d2c6de07fcdf75ac420dca0c0f45776704bfc9295593ac");

    private static CaseGenerator Accounts_1000_Storage_1() =>
        new(1000, 1, "b8bdf00f1f389a1445867e5c14ccf17fd21d915c01492bed3e70f74de7f42248");

    private static CaseGenerator Accounts_100_Storage_100() => new(100, 100,
        "0a1cba80719146d6a225a3d270a603d171d9792451ae628486563d42b96cc902");

    private class CaseGenerator
    {
        private readonly int _count;
        private readonly int _storageCount;
        public readonly string RootHash;
        public readonly Keccak RootHashAsKeccak;
        private uint _blocks;
        private Keccak _parent;

        public CaseGenerator(int count, int storageCount, string rootHash)
        {
            _count = count;
            _storageCount = storageCount;
            RootHash = rootHash;

            RootHashAsKeccak = new Keccak(Convert.FromHexString(RootHash));

            _blocks = 1;
            _parent = Keccak.EmptyTreeHash;
        }

        public void Run(Commit commit)
        {
            Run((ICommit)commit);
            commit.MergeAfterToBefore();
        }

        public void Run(ICommit commit)
        {
            var random = GetRandom();
            Span<byte> account = stackalloc byte[Account.MaxByteCount];

            for (var i = 0; i < _count; i++)
            {
                // account data first
                var keccak = random.NextKeccak();
                var value = (uint)random.Next();

                var a = new Account(value, value);
                commit.Set(Key.Account(keccak), a.WriteTo(account));

                // storage data second
                for (var j = 0; j < _storageCount; j++)
                {
                    var storageKey = random.NextKeccak();
                    var storageValue = random.Next();
                    commit.Set(Key.StorageCell(NibblePath.FromKey(keccak), storageKey), storageValue.ToByteArray());
                }
            }
        }

        public async Task<Keccak> Run(Blockchain blockchain, int newBlockEvery = int.MaxValue, bool delete = false,
            bool autoFinalize = false)
        {
            var counter = 0;
            using var worldState = blockchain.StartNew(_parent);

            var random = GetRandom();

            for (var i = 0; i < _count; i++)
            {
                // account data first
                var keccak = random.NextKeccak();
                var value = (uint)random.Next();

                var a = new Account(value, value);

                if (delete)
                {
                    worldState.DestroyAccount(keccak);
                }
                else
                {
                    worldState.SetAccount(keccak, a);
                }

                counter = await Next(counter, newBlockEvery, worldState, blockchain, autoFinalize);

                // storage data second
                for (var j = 0; j < _storageCount; j++)
                {
                    var storageKey = random.NextKeccak();
                    var storageValue = random.Next();

                    var actual = delete ? [] : storageValue.ToByteArray();
                    worldState.SetStorage(keccak, storageKey, actual);

                    counter = await Next(counter, newBlockEvery, worldState, blockchain, autoFinalize);
                }
            }

            return worldState.Commit(_blocks);
        }

        private async Task<int> Next(int counter, int newBlockEvery, IWorldState worldState, Blockchain blockchain,
            bool autoFinalize)
        {
            counter++;

            if (counter % newBlockEvery == 0)
            {
                counter = 0;
                _parent = worldState.Commit(_blocks++);

                if (autoFinalize)
                {
                    await blockchain.Finalize(_parent);
                }
            }

            return counter;
        }


        private static Random GetRandom() => new(13);
    }

    private static void AssertRoot(string hex, ICommitWithStats commit)
    {
        using var merkle = new ComputeMerkleBehavior();

        merkle.BeforeCommit(commit, CacheBudget.Options.None.Build());

        var keccak = new Keccak(Convert.FromHexString(hex));

        merkle.RootHash.Should().Be(keccak);
    }
}