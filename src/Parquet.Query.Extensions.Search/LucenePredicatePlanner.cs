using System.Collections.Concurrent;
using Parquet.Query.Planning;
using Parquet.Query.Pushdown;

namespace Parquet.Query.Extensions.Search;

public sealed class LucenePredicatePlanner<T> : IParquetPredicatePlanner<T>
    where T : class, new()
{
    private static readonly ConcurrentDictionary<string, CacheEntry> Cache = new(StringComparer.OrdinalIgnoreCase);

    public static LucenePredicatePlanner<T> Instance { get; } = new();

    public bool CanPlan(PushdownPredicate<T> predicate) => predicate is LuceneTermPredicate<T>;

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

    public ValueTask<PagePruningResult?> TryPrunePagesAsync(
        ParquetPagePruningContext context,
        PushdownPredicate<T> predicate,
        CancellationToken cancellationToken = default)
        => ValueTask.FromResult<PagePruningResult?>(null);

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
        if (Cache.TryGetValue(cacheKey, out var cached) &&
            cached.LastWriteTimeUtcTicks == lastWriteTimeUtc &&
            string.Equals(cached.Payload, payload, StringComparison.Ordinal))
        {
            return cached.Index;
        }

        var index = LuceneFooterIndexStorage.TryDeserialize(payload);
        Cache[cacheKey] = new CacheEntry(lastWriteTimeUtc, payload, index);
        return index;
    }

    private sealed record CacheEntry(long LastWriteTimeUtcTicks, string Payload, LuceneFooterIndexModel? Index);
}
