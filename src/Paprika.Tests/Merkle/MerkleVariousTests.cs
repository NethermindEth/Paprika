using System.Data;
using FluentAssertions;
using Paprika.Chain;
using Paprika.Crypto;
using Paprika.Data;
using Paprika.Merkle;

namespace Paprika.Tests.Merkle;

public class MerkleVariousTests
{
    [Test]
    public void Inspect([Values(true, false)] bool rlpMemo, [Values(true, false)] bool longKey)
    {
        var children2 = new NibbleSet.Readonly(0b1010_0000);
        var children3 = new NibbleSet.Readonly(0b1010_1000);
        var children4 = new NibbleSet.Readonly(0b1111_0000);
        var childrenAll = NibbleSet.Readonly.All;

        var merkle = new ComputeMerkleBehavior();

        var workingSet = new byte[1024];
        var commit = new Commit();

        var key = longKey ? Key.Merkle(NibblePath.Parse("12343456789")) : Key.Merkle(NibblePath.Parse("1"));
        var rlp = rlpMemo ? RlpMemo.Empty : ReadOnlySpan<byte>.Empty;

        Inspect(key, children2, rlp);
        Inspect(key, children3, rlp);
        Inspect(key, children4, rlp);
        Inspect(key, childrenAll, rlp);

        void Inspect(in Key key, NibbleSet.Readonly children, ReadOnlySpan<byte> rlp)
        {
            commit.SetBranch(key, children, rlp);
            var inspected = merkle.InspectBeforeApply(commit.Key, commit.Value, workingSet);
        }
    }

    class Commit : ICommit
    {
        private byte[] _key;
        private byte[] _value;

        public Key Key
        {
            get
            {
                var left = Key.ReadFrom(_key, out var key);
                left.Length.Should().Be(0);

                return key;
            }
        }

        public ReadOnlySpan<byte> Value => _value;

        public ReadOnlySpanOwnerWithMetadata<byte> Get(scoped in Key key)
        {
            throw new NotImplementedException();
        }

        public void Set(in Key key, in ReadOnlySpan<byte> payload, EntryType type = EntryType.Persistent)
        {
            Set(key, payload, ReadOnlySpan<byte>.Empty, type);
        }

        public void Set(in Key key, in ReadOnlySpan<byte> payload0, in ReadOnlySpan<byte> payload1, EntryType type = EntryType.Persistent)
        {
            _key = key.WriteTo(stackalloc byte[key.MaxByteLength]).ToArray();
            _value = new byte[payload0.Length + payload1.Length];

            payload0.CopyTo(_value);
            payload1.CopyTo(_value.AsSpan(payload0.Length));
        }

        public IChildCommit GetChild()
        {
            throw new NotImplementedException();
        }

        public IReadOnlyDictionary<Keccak, int> Stats => throw new RowNotInTableException();
    }
}