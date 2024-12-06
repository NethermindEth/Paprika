using FluentAssertions;
using NUnit.Framework;
using Paprika.Chain;
using Paprika.Crypto;
using Paprika.Data;
using Paprika.Merkle;
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

        using var block = blockchain.StartNew(Keccak.EmptyTreeHash);

        block.SetAccount(Key0, Account0);
        block.SetAccount(Key1A, Account1);
        block.SetAccount(Key2, Account2);

        block.Commit(1);
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

        public Keccak BeforeCommit(ICommitWithStats commit, CacheBudget budget)
        {
            _found.Clear();

            commit.Visit(OnKey, TrieType.State);

            _keccaks.SetEquals(_found).Should().BeTrue();

            return _found.Aggregate((keccak1, keccak2) =>
            {
                var result = default(Keccak);

                var span = result.BytesAsSpan;

                for (int i = 0; i < Keccak.Size; i++)
                {
                    span[i] = (byte)(keccak1.Span[i] ^ keccak2.Span[i] ^ i);
                }

                return result;
            });
        }

        private void OnKey(in Key key, ReadOnlySpan<byte> value)
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
