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
    public async Task Single_account()
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

    class AssertingKeysPreCommit : IPreCommitBehavior
    {
        private readonly HashSet<Keccak> _keccaks;

        public AssertingKeysPreCommit(HashSet<Keccak> keccaks)
        {
            _keccaks = keccaks;
        }

        public void BeforeCommit(ICommit commit)
        {
            foreach (var key in commit)
            {
                key.Type.Should().Be(DataType.Account);

                Keccak? found = null;
                foreach (var k in _keccaks)
                {
                    if (NibblePath.FromKey(k).Equals(key.Path))
                    {
                        found = k;
                        break;
                    }
                }

                found.Should().NotBeNull();
                _keccaks.Remove(found.Value);
            }

            _keccaks.Should().BeEmpty();
        }
    }
}