using System.Runtime.CompilerServices;
using Paprika.Crypto;
using Paprika.Data;
using Paprika.Merkle;
using Paprika.Utils;

namespace Paprika.Chain;

public static class CommitExtensions
{
    /// <summary>
    /// Creates a commit that caches specified reads so that data once reached,
    /// if they satisfy <paramref name="shouldCacheResult"/>, are cached locally.
    ///
    /// Writes are write through, but they are stored locally for faster compute as well.
    /// </summary>
    /// <returns>The wrapped commit.</returns>
    public static IChildCommit WriteThroughCacheChild(this IChildCommit original, ShouldCacheKey shouldCacheKey,
        ShouldCacheResult<byte> shouldCacheResult, BufferPool pool)
    {
        return new ReadCachingCommitChild(original, shouldCacheKey, shouldCacheResult, pool);
    }

    public static ISealableCommit WriteThroughCacheRoot(this ICommit original, ShouldCacheKey shouldCacheKey,
        ShouldCacheResult<byte> shouldCacheResult, BufferPool pool)
    {
        return new ReadCachingCommitRoot(original, shouldCacheKey, shouldCacheResult, pool);
    }

    private class ReadCachingCommitChild : ReadCachingCommit, IChildCommit
    {
        private readonly IChildCommit _commit;

        public ReadCachingCommitChild(IChildCommit commit, ShouldCacheKey shouldCacheKey, ShouldCacheResult<byte> shouldCacheResult, BufferPool pool)
            : base(commit, shouldCacheKey, shouldCacheResult, pool)
        {
            _commit = commit;
        }

        public override IChildCommit GetChild() => throw new NotImplementedException("Caching commit has no children");
        protected override void DisposeImpl() => _commit.Dispose();

        public void Commit() => _commit.Commit();
    }

    private class ReadCachingCommitRoot : ReadCachingCommit, ISealableCommit
    {
        private readonly ICommit _commit;
        private bool _canCache;

        public ReadCachingCommitRoot(ICommit commit, ShouldCacheKey shouldCacheKey, ShouldCacheResult<byte> shouldCacheResult, BufferPool pool)
            : base(commit, shouldCacheKey, shouldCacheResult, pool)
        {
            _commit = commit;
        }

        public override IChildCommit GetChild() => _commit.GetChild();
        public void SealCaching() => _canCache = false;

        public void Visit(CommitAction action, TrieType type) => _commit.Visit(action, type);

        protected override void DisposeImpl() { }

        protected override bool CanCache => _canCache;
    }

    private abstract class ReadCachingCommit : ICommit
    {
        private readonly ICommit _commit;
        private readonly ShouldCacheKey _shouldCacheKey;
        private readonly ShouldCacheResult<byte> _shouldCacheResult;
        private readonly PooledSpanDictionary _cache;

        protected ReadCachingCommit(ICommit commit, ShouldCacheKey shouldCacheKey,
            ShouldCacheResult<byte> shouldCacheResult, BufferPool pool)
        {
            _commit = commit;
            _shouldCacheKey = shouldCacheKey;
            _shouldCacheResult = shouldCacheResult;
            _cache = new PooledSpanDictionary(pool, true, false);
        }

        public ReadOnlySpanOwnerWithMetadata<byte> Get(scoped in Key key)
        {
            if (ShouldCache(key) == false)
            {
                return _commit.Get(key);
            }

            var hash = Hash(key);
            var keyWritten = key.WriteTo(stackalloc byte[key.MaxByteLength]);

            if (_cache.TryGet(keyWritten, hash, out var cached))
            {
                // return cached
                return new ReadOnlySpanOwner<byte>(cached, null).WithDepth(0);
            }

            var result = _commit.Get(key);

            if (_shouldCacheResult(result))
            {
                _cache.Set(keyWritten, hash, result.Span);
            }

            return result;
        }

        private bool ShouldCache(in Key key) => CanCache && _shouldCacheKey(key);

        [SkipLocalsInit]
        public void Set(in Key key, in ReadOnlySpan<byte> payload)
        {
            if (ShouldCache(key))
            {
                // write locally
                var keyWritten = key.WriteTo(stackalloc byte[key.MaxByteLength]);
                _cache.Set(keyWritten, Hash(key), payload);
            }

            // write in the wrapped
            _commit.Set(key, payload);
        }

        [SkipLocalsInit]
        public void Set(in Key key, in ReadOnlySpan<byte> payload0, in ReadOnlySpan<byte> payload1)
        {
            if (ShouldCache(key))
            {
                // write locally
                var keyWritten = key.WriteTo(stackalloc byte[key.MaxByteLength]);
                _cache.Set(keyWritten, Hash(key), payload0, payload1);
            }

            // write in the wrapped
            _commit.Set(key, payload0, payload1);
        }

        private static int Hash(in Key key) => Blockchain.GetHash(key);

        public abstract IChildCommit GetChild();

        public IReadOnlyDictionary<Keccak, int> Stats => _commit.Stats;

        public void Dispose()
        {
            _cache.Dispose();
            DisposeImpl();
        }

        protected abstract void DisposeImpl();

        protected virtual bool CanCache => false;
    }
}

/// <summary>
/// The predicate whether the key should be cached.
/// </summary>
public delegate bool ShouldCacheKey(in Key key);

/// <summary>
/// The predicate whether the result should be cached.
/// </summary>
public delegate bool ShouldCacheResult<T>(in ReadOnlySpanOwnerWithMetadata<T> result);

public interface ISealableCommit : ICommit, IDisposable
{
    void SealCaching();
}