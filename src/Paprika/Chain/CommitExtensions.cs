using System.Runtime.CompilerServices;
using Paprika.Crypto;
using Paprika.Data;
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
    public static IChildCommit WriteThroughCache(this IChildCommit original, ShouldCacheKey shouldCacheKey,
        ShouldCacheResult<byte> shouldCacheResult, BufferPool pool)
    {
        return new ReadCachingCommit(original, shouldCacheKey, shouldCacheResult, pool);
    }

    private class ReadCachingCommit : IChildCommit
    {
        private readonly IChildCommit _commit;
        private readonly ShouldCacheKey _shouldCacheKey;
        private readonly ShouldCacheResult<byte> _shouldCacheResult;
        private readonly PooledSpanDictionary _cache;

        public ReadCachingCommit(IChildCommit commit, ShouldCacheKey shouldCacheKey,
            ShouldCacheResult<byte> shouldCacheResult, BufferPool pool)
        {
            _commit = commit;
            _shouldCacheKey = shouldCacheKey;
            _shouldCacheResult = shouldCacheResult;
            _cache = new PooledSpanDictionary(pool, true, false);
        }

        public ReadOnlySpanOwnerWithMetadata<byte> Get(scoped in Key key)
        {
            if (_shouldCacheKey(key) == false)
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

        [SkipLocalsInit]
        public void Set(in Key key, in ReadOnlySpan<byte> payload)
        {
            if (_shouldCacheKey(key))
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
            if (_shouldCacheKey(key))
            {
                // write locally
                var keyWritten = key.WriteTo(stackalloc byte[key.MaxByteLength]);
                _cache.Set(keyWritten, Hash(key), payload0, payload1);
            }

            // write in the wrapped
            _commit.Set(key, payload0, payload1);
        }

        private static int Hash(in Key key) => Blockchain.GetHash(key);

        public IChildCommit GetChild() => throw new NotImplementedException("Caching commit has no children");

        public IReadOnlyDictionary<Keccak, int> Stats => _commit.Stats;

        public void Dispose()
        {
            _cache.Dispose();
            _commit.Dispose();
        }

        public void Commit() => _commit.Commit();
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