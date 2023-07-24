using NUnit.Framework;
using Paprika.Chain;
using Paprika.Data;
using Paprika.Merkle;
using Paprika.Utils;

namespace Paprika.Tests.Merkle;

public class DirtyTests
{
    [Test]
    public void Empty()
    {
        var merkle = new ComputeMerkleBehavior();
        merkle.BeforeCommit(new Commit());
    }

    class Commit : ICommit
    {
        private readonly Dictionary<byte[], byte[]> _before = new(new BytesEqualityComparer());
        private readonly Dictionary<byte[], byte[]> _after = new(new BytesEqualityComparer());

        public ReadOnlySpanOwner<byte> Get(in Key key)
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

        public void Set(in Key key, in ReadOnlySpan<byte> payload)
        {
            _after[GetKey(key)] = payload.ToArray();
        }

        public void Visit(CommitAction action)
        {
            foreach (var (k, _) in _before)
            {
                Key.ReadFrom(k, out var key);
                action(key, this);
            }
        }
    }
}