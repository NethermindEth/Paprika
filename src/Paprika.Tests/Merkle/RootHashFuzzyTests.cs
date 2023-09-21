using FluentAssertions;
using NUnit.Framework;
using Paprika.Chain;
using Paprika.Crypto;
using Paprika.Data;
using Paprika.Merkle;

namespace Paprika.Tests.Merkle;

public class RootHashFuzzyTests
{
    [TestCase(1000, "b255eb6261dc19f0639d13624e384b265759d2e4171c0eb9487e82d2897729f0")]
    [TestCase(10_000, "48864c880bd7610f9bad9aff765844db83c17cab764f5444b43c0076f6cf6c03")]
    public void Big_random(int count, string hexString)
    {
        var generator = new CaseGenerator(count, 0);

        var commit = new Commit();

        generator.Run(commit);

        // assert twice to ensure that the root is not changed
        AssertRoot(hexString, commit);

        AssertRoot(hexString, commit);
    }

    [TestCase(1, 1, "954f21233681f1b941ef67b30c85b64bfb009452b7f01b28de28eb4c1d2ca258")]
    [TestCase(1, 100, "c8cf5e6b84e39beeac713a42546cc977581d9b31307efa2b1b288ccd828f278e")]
    [TestCase(100, 1, "68965a86aec45d3863d2c6de07fcdf75ac420dca0c0f45776704bfc9295593ac")]
    [TestCase(1000, 1, "b8bdf00f1f389a1445867e5c14ccf17fd21d915c01492bed3e70f74de7f42248")]
    [TestCase(1000, 1000, "4f474648522dc59d4d4a918e301d9d36ac200029027d28605cd2ab32f37321f8")]
    public void Big_random_storage(int count, int storageCount, string hexString)
    {
        var generator = new CaseGenerator(count, storageCount);

        var commit = new Commit();
        generator.Run(commit);

        AssertRoot(hexString, commit);
    }

    class CaseGenerator
    {
        private readonly int _count;
        private readonly int _storageCount;

        public CaseGenerator(int count, int storageCount)
        {
            _count = count;
            _storageCount = storageCount;
        }

        public void Run(Commit commit)
        {
            Run((ICommit)commit);
            commit.MergeAfterToBefore();
        }

        public void Run(ICommit commit)
        {
            Random random = new(13);
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
    }
    private static void AssertRoot(string hex, ICommit commit)
    {
        var merkle = new ComputeMerkleBehavior(true);

        merkle.BeforeCommit(commit);

        var keccak = new Keccak(Convert.FromHexString(hex));

        merkle.RootHash.Should().Be(keccak);
    }
}