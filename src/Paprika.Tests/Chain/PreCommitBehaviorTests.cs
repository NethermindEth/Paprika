using FluentAssertions;
using NUnit.Framework;
using Paprika.Chain;
using Paprika.Crypto;
using Paprika.Data;
using Paprika.Store;
using static Paprika.Tests.Values;

namespace Paprika.Tests.Chain;

public class PreCommitBehaviorTests
{
    private const int SmallDb = 64 * 1024;
    private static readonly Keccak BlockKeccak = Keccak.Compute("block"u8);

    private static readonly Account Account0 = new(Balance0, Nonce0);
    private static readonly Account Account1 = new(Balance1, Nonce1);
    private static readonly Account Account2 = new(Balance2, Nonce2);

    [Test]
    public async Task Multiple_keys_enumerated()
    {
        using var db = PagedDb.NativeMemoryDb(SmallDb);

        var preCommit = new AssertingKeysPreCommit(new HashSet<Keccak> { Key0, Key1A, Key2 });

        await using var blockchain = new Blockchain(db, preCommit: preCommit);

        using var block = blockchain.StartNew(Keccak.Zero, BlockKeccak, 1);

        block.SetAccount(Key0, Account0);
        block.SetAccount(Key1A, Account1);
        block.SetAccount(Key2, Account2);

        block.Commit();
    }

    [Test]
    public async Task Get_set_work_only_in_pre_commit_hook()
    {
        using var db = PagedDb.NativeMemoryDb(SmallDb);

        await using var blockchain = new Blockchain(db, new SetGetPreCommitBehavior());

        using var block = blockchain.StartNew(Keccak.Zero, BlockKeccak, 1);

        // no values as they are added in the hook only

        block.Commit();
    }

    /// <summary>
    /// Asserts that the given set of keys was written
    /// </summary>
    class SetGetPreCommitBehavior : IPreCommitBehavior
    {
        public static Key AssignedKey => Key.Merkle(NibblePath.FromKey(Key2));

        public static ReadOnlySpan<byte> Value => new byte[29];

        public void BeforeCommit(ICommit commit)
        {
            commit.Set(AssignedKey, Value);

            commit.Visit(OnKey);

            using var owner = commit.Get(AssignedKey);

            owner.IsEmpty.Should().BeFalse();
            owner.Span.SequenceEqual(Value).Should().BeTrue();
        }

        private static void OnKey(in Key key, ReadOnlySpan<byte> value, ICommit commit) => throw new Exception("Should not be called at all!");
    }

    /// <summary>
    /// Asserts that the given set of keys was written
    /// </summary>
    class AssertingKeysPreCommit : IPreCommitBehavior
    {
        private readonly HashSet<Keccak> _keccaks;
        private readonly HashSet<Keccak> _found;

        public AssertingKeysPreCommit(HashSet<Keccak> keccaks)
        {
            _keccaks = keccaks;
            _found = new HashSet<Keccak>();
        }

        public void BeforeCommit(ICommit commit)
        {
            _found.Clear();

            commit.Visit(OnKey);

            _keccaks.SetEquals(_found).Should().BeTrue();
        }

        private void OnKey(in Key key, ReadOnlySpan<byte> value, ICommit commit)
        {
            key.Type.Should().Be(DataType.Account);

            foreach (var k in _keccaks)
            {
                if (NibblePath.FromKey(k).Equals(key.Path))
                {
                    _found.Add(k);
                    break;
                }
            }
        }
    }
}