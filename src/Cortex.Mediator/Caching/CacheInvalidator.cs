using Cortex.Mediator.Queries;
using Microsoft.Extensions.Caching.Memory;
using Microsoft.Extensions.Primitives;
using System;
using System.Collections.Concurrent;
using System.Threading;

namespace Cortex.Mediator.Caching
{
    /// <summary>
    /// Interface for invalidating cached query results.
    /// </summary>
    public interface ICacheInvalidator
    {
        /// <summary>
        /// Invalidates the cached result for a specific query.
        /// </summary>
        /// <typeparam name="TQuery">The type of query.</typeparam>
        /// <typeparam name="TResult">The type of result.</typeparam>
        /// <param name="query">The query whose cached result should be invalidated.</param>
        void Invalidate<TQuery, TResult>(TQuery query) where TQuery : IQuery<TResult>;

        /// <summary>
        /// Invalidates all cached results for a specific query type.
        /// </summary>
        /// <typeparam name="TQuery">The type of query.</typeparam>
        /// <typeparam name="TResult">The type of result.</typeparam>
        void InvalidateAll<TQuery, TResult>() where TQuery : IQuery<TResult>;

        /// <summary>
        /// Invalidates a cached result by its cache key.
        /// </summary>
        /// <param name="cacheKey">The cache key to invalidate.</param>
        void InvalidateByKey(string cacheKey);
    }

    /// <summary>
    /// Default implementation of cache invalidator using IMemoryCache.
    /// </summary>
    /// <remarks>
    /// <see cref="InvalidateAll{TQuery, TResult}"/> works for entries cached by <c>CachingQueryBehavior</c>
    /// while this instance is the registered <see cref="ICacheInvalidator"/>: the behavior attaches a
    /// per-query-type expiration token to each entry, and <c>InvalidateAll</c> cancels that token.
    /// </remarks>
    public class CacheInvalidator : ICacheInvalidator
    {
        private readonly IMemoryCache _cache;
        private readonly ICacheKeyGenerator _cacheKeyGenerator;
        private readonly ConcurrentDictionary<Type, CancellationTokenSource> _tokenSources = new();

        public CacheInvalidator(IMemoryCache cache, ICacheKeyGenerator cacheKeyGenerator)
        {
            _cache = cache ?? throw new ArgumentNullException(nameof(cache));
            _cacheKeyGenerator = cacheKeyGenerator ?? throw new ArgumentNullException(nameof(cacheKeyGenerator));
        }

        public void Invalidate<TQuery, TResult>(TQuery query) where TQuery : IQuery<TResult>
        {
            var cacheKey = _cacheKeyGenerator.GenerateKey<TQuery, TResult>(query);
            InvalidateByKey(cacheKey);
        }

        public void InvalidateAll<TQuery, TResult>() where TQuery : IQuery<TResult>
        {
            // The next GetExpirationToken call creates a fresh source, so later entries are cached again.
            // The removed source is not disposed: an entry being cached right now may still register on its token.
            if (_tokenSources.TryRemove(typeof(TQuery), out var source))
            {
                source.Cancel();
            }
        }

        public void InvalidateByKey(string cacheKey)
        {
            if (string.IsNullOrEmpty(cacheKey))
                throw new ArgumentException("Cache key cannot be null or empty", nameof(cacheKey));

            _cache.Remove(cacheKey);
        }

        /// <summary>
        /// Returns the token that expires every cached result of the given query type when
        /// <see cref="InvalidateAll{TQuery, TResult}"/> is called for it.
        /// This method is called internally by the caching behavior.
        /// </summary>
        internal IChangeToken GetExpirationToken(Type queryType)
        {
            var source = _tokenSources.GetOrAdd(queryType, _ => new CancellationTokenSource());
            return new CancellationChangeToken(source.Token);
        }
    }
}
