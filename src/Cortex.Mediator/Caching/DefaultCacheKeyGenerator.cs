using Cortex.Mediator.Queries;
using Microsoft.Extensions.Options;
using System;
using System.Linq;
using System.Reflection;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;

namespace Cortex.Mediator.Caching
{
    /// <summary>
    /// Default implementation of cache key generator.
    /// Generates cache keys based on query type and properties.
    /// </summary>
    public class DefaultCacheKeyGenerator : ICacheKeyGenerator
    {
        private readonly CachingOptions _options;
        private static readonly JsonSerializerOptions JsonOptions = new()
        {
            PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
            WriteIndented = false
        };

        public DefaultCacheKeyGenerator(IOptions<CachingOptions> options)
        {
            _options = options?.Value ?? new CachingOptions();
        }

        public DefaultCacheKeyGenerator() : this(null!)
        {
        }

        public string GenerateKey<TQuery, TResult>(TQuery query) where TQuery : IQuery<TResult>
        {
            if (query == null)
                throw new ArgumentNullException(nameof(query));

            var queryType = typeof(TQuery);
            var prefix = _options.CacheKeyPrefix;

            // Full name with generic arguments and without assembly versions, e.g. "Sales.GetTotalQuery" or
            // "Reports.GetPageQuery`1[System.Int32]". Type.Name alone collides across namespaces and generic arguments.
            var queryTypeName = queryType.ToString();

            // Check for CacheableAttribute with custom prefix
            var cacheableAttr = queryType.GetCustomAttribute<CacheableAttribute>();
            if (cacheableAttr?.CacheKeyPrefix != null)
            {
                prefix = cacheableAttr.CacheKeyPrefix;
            }

            // Check if query implements ICacheableQuery with custom key
            if (query is ICacheableQuery cacheableQuery && !string.IsNullOrEmpty(cacheableQuery.CacheKey))
            {
                return $"{prefix}:{queryTypeName}:{cacheableQuery.CacheKey}";
            }

            // Generate key from query properties
            if (_options.IncludeQueryPropertiesInCacheKey)
            {
                var queryHash = GenerateQueryHash(query);
                return $"{prefix}:{queryTypeName}:{queryHash}";
            }

            return $"{prefix}:{queryTypeName}";
        }

        private static string GenerateQueryHash<TQuery>(TQuery query)
        {
            try
            {
                var json = JsonSerializer.Serialize(query, JsonOptions);
                var bytes = Encoding.UTF8.GetBytes(json);

#if NETSTANDARD2_0 || NETSTANDARD2_1
                using var sha256 = SHA256.Create();
                var hash = sha256.ComputeHash(bytes);
#else
                var hash = SHA256.HashData(bytes);
#endif

                return Convert.ToBase64String(hash)
                    .Replace("+", "-")
                    .Replace("/", "_")
                    .TrimEnd('=');
            }
            catch
            {
                // Fallback to GetHashCode if serialization fails
                return query!.GetHashCode().ToString("X8");
            }
        }
    }
}
