using System.Collections.Concurrent;
using Parquet.Query.Internal;
using Parquet.Query.Planning;
using Parquet.Query.Pushdown;

namespace Parquet.Query.Extensions.Search;

/// <summary>
/// Uses Lucene footer indexes to prune row groups for term and fuzzy term predicates.
/// </summary>
/// <typeparam name="T">The source row type the planner targets.</typeparam>
public sealed class LucenePredicatePlanner<T> : IParquetPredicatePlanner<T>
    where T : class, new()
{
    private const int MaxCacheEntries = 256;
    private static readonly ConcurrentDictionary<string, CacheEntry> Cache = new(StringComparer.OrdinalIgnoreCase);

    /// <summary>
    /// Gets a shared planner instance for the source type.
    /// </summary>
    public static LucenePredicatePlanner<T> Instance { get; } = new();

    /// <inheritdoc />
    public bool CanPlan(PushdownPredicate<T> predicate) => predicate is LuceneTermPredicate<T>;

    /// <inheritdoc />
    public RowGroupPredicateDecision? TryEvaluateRowGroup(
        ParquetRowGroupPlannerContext context,
        PushdownPredicate<T> predicate)
    {
        if (predicate is not LuceneTermPredicate<T> lucenePredicate)
        {
            return null;
        }

        var index = GetIndex(context.FilePath, context.Reader.CustomMetadata, lucenePredicate.ColumnPath);
        if (index is null || context.RowGroupIndex >= index.RowGroups.Count)
        {
            return null;
        }

        var rowGroupTermOrdinals = index.RowGroups[context.RowGroupIndex];
        var matchingTerms = rowGroupTermOrdinals
            .Select(ordinal => index.Terms[ordinal])
            .Where(candidate => LuceneEditDistance.IsMatch(
                candidate,
                lucenePredicate.Term,
                lucenePredicate.MaxEdits,
                lucenePredicate.PrefixLength,
                lucenePredicate.Transpositions))
            .Take(8)
            .ToArray();

        var mayMatch = matchingTerms.Length > 0;
        return new RowGroupPredicateDecision(
            predicate.Description,
            mayMatch,
            "lucene",
            mayMatch
                ? $"Indexed terms matched: {string.Join(", ", matchingTerms)}."
                : "The lucene footer term dictionary ruled the row group out.");
    }

    /// <inheritdoc />
    public ValueTask<PagePruningResult?> TryPrunePagesAsync(
        ParquetPagePruningContext context,
        PushdownPredicate<T> predicate,
        CancellationToken cancellationToken = default)
        => new ValueTask<PagePruningResult?>((PagePruningResult?)null);

    private static LuceneFooterIndexModel? GetIndex(
        string filePath,
        IReadOnlyDictionary<string, string> metadata,
        string columnPath)
    {
        var metadataKey = LuceneFooterIndexStorage.GetMetadataKey(columnPath);
        if (!metadata.TryGetValue(metadataKey, out var payload))
        {
            return null;
        }

        var lastWriteTimeUtc = System.IO.File.GetLastWriteTimeUtc(filePath).Ticks;
        var cacheKey = $"{filePath}\n{metadataKey}";
        var accessTimeUtc = DateTime.UtcNow.Ticks;
        if (Cache.TryGetValue(cacheKey, out var cached) &&
            cached.LastWriteTimeUtcTicks == lastWriteTimeUtc &&
            string.Equals(cached.Payload, payload, StringComparison.Ordinal))
        {
            var refreshed = cached with { LastAccessUtcTicks = accessTimeUtc };
            BoundedPlannerCache.Set(Cache, cacheKey, refreshed, MaxCacheEntries, static entry => entry.LastAccessUtcTicks);
            return refreshed.Index;
        }

        var index = LuceneFooterIndexStorage.TryDeserialize(payload);
        BoundedPlannerCache.Set(
            Cache,
            cacheKey,
            new CacheEntry(accessTimeUtc, lastWriteTimeUtc, payload, index),
            MaxCacheEntries,
            static entry => entry.LastAccessUtcTicks);
        return index;
    }

    private sealed record CacheEntry(long LastAccessUtcTicks, long LastWriteTimeUtcTicks, string Payload, LuceneFooterIndexModel? Index);
}
