using System.Collections.Concurrent;
using Parquet.Query.Internal;
using Parquet.Query.Planning;
using Parquet.Query.Pushdown;

namespace Parquet.Query.Extensions.Indexing;

/// <summary>
/// Uses footer bitmap indexes to prune row groups for equality predicates.
/// </summary>
/// <typeparam name="T">The source row type the planner targets.</typeparam>
public sealed class FooterIndexPredicatePlanner<T> : IParquetPredicatePlanner<T>
    where T : class, new()
{
    private const int MaxCacheEntries = 256;
    private static readonly ConcurrentDictionary<string, CacheEntry> BitmapCache = new(StringComparer.OrdinalIgnoreCase);

    /// <summary>
    /// Gets a shared planner instance for the source type.
    /// </summary>
    public static FooterIndexPredicatePlanner<T> Instance { get; } = new();

    /// <inheritdoc />
    public bool CanPlan(PushdownPredicate<T> predicate) =>
        predicate is ComparisonPushdownPredicate<T> comparison &&
        comparison.Operator == ComparisonOperator.Equal &&
        comparison.Value is not null;

    /// <inheritdoc />
    public RowGroupPredicateDecision? TryEvaluateRowGroup(
        ParquetRowGroupPlannerContext context,
        PushdownPredicate<T> predicate)
    {
        if (predicate is not ComparisonPushdownPredicate<T> comparison ||
            comparison.Operator != ComparisonOperator.Equal ||
            comparison.Value is null)
        {
            return null;
        }

        var index = GetBitmapIndex(context.FilePath, context.Reader.CustomMetadata, predicate.ColumnPath);
        if (index is null || !FooterIndexValueFormatter.TryFormat(comparison.Value, out var value))
        {
            return null;
        }

        var entry = index.Values.FirstOrDefault(candidate => string.Equals(candidate.Value, value, StringComparison.Ordinal));
        if (entry is null)
        {
            return new RowGroupPredicateDecision(
                predicate.Description,
                mayMatch: false,
                source: "footer-bitmap",
                reason: $"The footer bitmap index does not contain '{value}'.");
        }

        var mayMatch = Array.BinarySearch(entry.RowGroups, context.RowGroupIndex) >= 0;
        return new RowGroupPredicateDecision(
            predicate.Description,
            mayMatch,
            "footer-bitmap",
            mayMatch
                ? $"The footer bitmap index includes row group {context.RowGroupIndex} for '{value}'."
                : $"The footer bitmap index ruled row group {context.RowGroupIndex} out for '{value}'.");
    }

    /// <inheritdoc />
    public ValueTask<PagePruningResult?> TryPrunePagesAsync(
        ParquetPagePruningContext context,
        PushdownPredicate<T> predicate,
        CancellationToken cancellationToken = default)
        => new ValueTask<PagePruningResult?>((PagePruningResult?)null);

    private static FooterBitmapIndexModel? GetBitmapIndex(
        string filePath,
        IReadOnlyDictionary<string, string> metadata,
        string columnPath)
    {
        var metadataKey = FooterIndexStorage.GetBitmapMetadataKey(columnPath);
        if (!metadata.TryGetValue(metadataKey, out var payload))
        {
            return null;
        }

        var lastWriteTimeUtc = System.IO.File.GetLastWriteTimeUtc(filePath).Ticks;
        var cacheKey = $"{filePath}\n{metadataKey}";
        var accessTimeUtc = DateTime.UtcNow.Ticks;
        if (BitmapCache.TryGetValue(cacheKey, out var cached) &&
            cached.LastWriteTimeUtcTicks == lastWriteTimeUtc &&
            string.Equals(cached.Payload, payload, StringComparison.Ordinal))
        {
            var refreshed = cached with { LastAccessUtcTicks = accessTimeUtc };
            BoundedPlannerCache.Set(BitmapCache, cacheKey, refreshed, MaxCacheEntries, static entry => entry.LastAccessUtcTicks);
            return refreshed.Index;
        }

        var index = FooterIndexStorage.TryDeserialize<FooterBitmapIndexModel>(payload);
        BoundedPlannerCache.Set(
            BitmapCache,
            cacheKey,
            new CacheEntry(accessTimeUtc, lastWriteTimeUtc, payload, index),
            MaxCacheEntries,
            static entry => entry.LastAccessUtcTicks);
        return index;
    }

    private sealed record CacheEntry(long LastAccessUtcTicks, long LastWriteTimeUtcTicks, string Payload, FooterBitmapIndexModel? Index);
}
