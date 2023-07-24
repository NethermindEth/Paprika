using FluentAssertions;
using NUnit.Framework;
using Paprika.Chain;
using Paprika.Data;
using Paprika.Merkle;
using Paprika.Utils;

using static Paprika.Tests.Values;

namespace Paprika.Tests.Merkle;

public class DirtyTests
{
    [Test]
    public void Empty()
    {
        var merkle = new ComputeMerkleBehavior();
        var commit = new Commit();

        merkle.BeforeCommit(commit);

        commit.ShouldBeEmpty();
    }

    [Test]
    public void Single_account()
    {
        var account = Key.Account(Key2);

        var merkle = new ComputeMerkleBehavior();
        var commit = new Commit();
        commit.Set(account, new byte[] { 1 });

        merkle.BeforeCommit(commit);

        commit.ShouldBeEmpty();
    }

    class Commit : ICommit
    {
        private readonly Dictionary<byte[], byte[]> _before = new(new BytesEqualityComparer());
        private readonly Dictionary<byte[], byte[]> _after = new(new BytesEqualityComparer());

        public void Set(in Key key, ReadOnlySpan<byte> value)
        {
            _before[GetKey(key)] = value.ToArray();
        }

        public void ShouldWrite(in Key key, ReadOnlySpan<byte> value)
        {
            _after.Remove(GetKey(key), out var existing).Should().BeTrue();
            value.SequenceEqual(existing).Should().BeTrue();
        }

        public void ShouldBeEmpty() => _after.Count.Should().Be(0);

        ReadOnlySpanOwner<byte> ICommit.Get(in Key key)
        {
            var k = GetKey(key);
            if (_before.TryGetValue(k, out var value))
            {
                return new ReadOnlySpanOwner<byte>(value, null);
            }
            if (_after.TryGetValue(k, out value))
            {
                return new ReadOnlySpanOwner<byte>(value, null);
            }

            return default;
        }

        private static byte[] GetKey(in Key key) => key.WriteTo(stackalloc byte[key.MaxByteLength]).ToArray();

        void ICommit.Set(in Key key, in ReadOnlySpan<byte> payload)
        {
            _after[GetKey(key)] = payload.ToArray();
        }

        void ICommit.Visit(CommitAction action)
        {
            foreach (var (k, _) in _before)
            {
                Key.ReadFrom(k, out var key);
                action(key, this);
            }
        }
    }
}