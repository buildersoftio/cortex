using Cortex.Mediator.Caching;
using Cortex.Mediator.Queries;
using Microsoft.Extensions.Caching.Memory;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using System;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;

namespace Cortex.Mediator.Behaviors
{
    /// <summary>
    /// Pipeline behavior that caches query results.
    /// Queries must implement ICacheableQuery or be decorated with CacheableAttribute to be cached.
    /// </summary>
    /// <typeparam name="TQuery">The type of query being handled.</typeparam>
    /// <typeparam name="TResult">The type of result returned by the query.</typeparam>
    public sealed class CachingQueryBehavior<TQuery, TResult> : IQueryPipelineBehavior<TQuery, TResult>
        where TQuery : IQuery<TResult>
    {
        private readonly IMemoryCache _cache;
        private readonly ICacheKeyGenerator _cacheKeyGenerator;
        private readonly ILogger<CachingQueryBehavior<TQuery, TResult>> _logger;
        private readonly CachingOptions _options;
        private readonly CacheInvalidator? _cacheInvalidator;

        /// <param name="cache">The memory cache.</param>
        /// <param name="cacheKeyGenerator">The cache key generator.</param>
        /// <param name="logger">The logger.</param>
        /// <param name="options">The caching options.</param>
        /// <param name="cacheInvalidator">
        /// The registered invalidator. When it is the built-in <see cref="CacheInvalidator"/>, cached entries
        /// carry its per-query-type token so <see cref="ICacheInvalidator.InvalidateAll{TQuery, TResult}"/> removes them.
        /// </param>
        public CachingQueryBehavior(
            IMemoryCache cache,
            ICacheKeyGenerator cacheKeyGenerator,
            ILogger<CachingQueryBehavior<TQuery, TResult>> logger,
            IOptions<CachingOptions> options,
            ICacheInvalidator? cacheInvalidator = null)
        {
            _cache = cache ?? throw new ArgumentNullException(nameof(cache));
            _cacheKeyGenerator = cacheKeyGenerator ?? throw new ArgumentNullException(nameof(cacheKeyGenerator));
            _logger = logger ?? throw new ArgumentNullException(nameof(logger));
            _options = options?.Value ?? new CachingOptions();
            _cacheInvalidator = cacheInvalidator as CacheInvalidator;
        }

        public async Task<TResult> Handle(
            TQuery query,
            QueryHandlerDelegate<TResult> next,
            CancellationToken cancellationToken)
        {
            // Check if caching is enabled globally
            if (!_options.EnableCaching)
            {
                return await next();
            }

            // Check if the query is cacheable
            if (!IsCacheable(query, out var absoluteExpiration, out var slidingExpiration))
            {
                return await next();
            }

            var cacheKey = _cacheKeyGenerator.GenerateKey<TQuery, TResult>(query);

            // Try to get from cache
            if (_cache.TryGetValue(cacheKey, out TResult? cachedResult))
            {
                _logger.LogDebug(
                    "Cache hit for query {QueryName} with key {CacheKey}",
                    typeof(TQuery).Name,
                    cacheKey);

                return cachedResult!;
            }

            _logger.LogDebug(
                "Cache miss for query {QueryName} with key {CacheKey}",
                typeof(TQuery).Name,
                cacheKey);

            // Take the invalidation token before running the query, so an InvalidateAll that happens
            // while the handler runs also expires this result.
            var expirationToken = _cacheInvalidator?.GetExpirationToken(typeof(TQuery));

            // Execute the query
            var result = await next();

            // Cache the result
            if (result != null)
            {
                var cacheEntryOptions = new MemoryCacheEntryOptions
                {
                    AbsoluteExpirationRelativeToNow = absoluteExpiration,
                    SlidingExpiration = slidingExpiration
                };

                if (expirationToken != null)
                {
                    cacheEntryOptions.AddExpirationToken(expirationToken);
                }

                _cache.Set(cacheKey, result, cacheEntryOptions);

                _logger.LogDebug(
                    "Cached result for query {QueryName} with key {CacheKey}, expires in {Expiration}",
                    typeof(TQuery).Name,
                    cacheKey,
                    absoluteExpiration);
            }

            return result;
        }

        private bool IsCacheable(TQuery query, out TimeSpan absoluteExpiration, out TimeSpan slidingExpiration)
        {
            absoluteExpiration = _options.DefaultAbsoluteExpiration;
            slidingExpiration = _options.DefaultSlidingExpiration;

            // Check if query implements ICacheableQuery
            if (query is ICacheableQuery cacheableQuery)
            {
                absoluteExpiration = cacheableQuery.AbsoluteExpiration ?? _options.DefaultAbsoluteExpiration;
                slidingExpiration = cacheableQuery.SlidingExpiration ?? _options.DefaultSlidingExpiration;
                return true;
            }

            // Check for CacheableAttribute
            var cacheableAttr = typeof(TQuery).GetCustomAttribute<CacheableAttribute>();
            if (cacheableAttr != null)
            {
                absoluteExpiration = cacheableAttr.AbsoluteExpiration;
                slidingExpiration = cacheableAttr.SlidingExpiration;
                return true;
            }

            return false;
        }
    }
}
