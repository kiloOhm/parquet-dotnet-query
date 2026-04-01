using System.Globalization;
using Parquet.Query.Internal;
using Parquet.Query.Planning;
using Parquet.Query.Pushdown;

namespace Parquet.Query.Dynamic;

/// <summary>
/// Uses footer indexes (Lucene, bitmap) to prune row groups for dynamic queries.
/// </summary>
internal sealed class DynamicFooterIndexPlanner : IParquetPredicatePlanner<DynamicRow>
{
    private const string LuceneMetadataPrefix = "parquet.query.lucene.v1/";
    private const string BitmapMetadataPrefix = "parquet.query.index.bitmap.v1/";

    public static DynamicFooterIndexPlanner Instance { get; } = new();

    public bool CanPlan(PushdownPredicate<DynamicRow> predicate) =>
        predicate is DynamicLuceneTermPredicate ||
        (predicate is ComparisonPushdownPredicate<DynamicRow> comparison &&
         comparison.Operator == ComparisonOperator.Equal &&
         comparison.Value is not null);

    public RowGroupPredicateDecision? TryEvaluateRowGroup(
        ParquetRowGroupPlannerContext context,
        PushdownPredicate<DynamicRow> predicate)
    {
        if (predicate is DynamicLuceneTermPredicate lucene)
            return TryEvaluateLucene(context, lucene);

        if (predicate is ComparisonPushdownPredicate<DynamicRow> comparison &&
            comparison.Operator == ComparisonOperator.Equal &&
            comparison.Value is not null)
        {
            return TryEvaluateBitmap(context, predicate, comparison);
        }

        return null;
    }

    public ValueTask<PagePruningResult?> TryPrunePagesAsync(
        ParquetPagePruningContext context,
        PushdownPredicate<DynamicRow> predicate,
        CancellationToken cancellationToken = default)
        => new ValueTask<PagePruningResult?>((PagePruningResult?)null);

    private static RowGroupPredicateDecision? TryEvaluateLucene(
        ParquetRowGroupPlannerContext context,
        DynamicLuceneTermPredicate predicate)
    {
        var metadataKey = LuceneMetadataPrefix + Uri.EscapeDataString(predicate.ColumnPath);
        if (!context.Reader.CustomMetadata.TryGetValue(metadataKey, out var payload))
            return null;

        var index = ParquetFooterMetadata.TryDeserialize<LuceneIndexModel>(payload);
        if (index is null || context.RowGroupIndex >= index.RowGroups.Count)
            return null;

        var rowGroupTermOrdinals = index.RowGroups[context.RowGroupIndex];
        var matchingTerms = rowGroupTermOrdinals
            .Where(ordinal => ordinal >= 0 && ordinal < index.Terms.Count)
            .Select(ordinal => index.Terms[ordinal])
            .Where(candidate => DynamicPredicateCompiler.IsTermMatch(
                candidate,
                predicate.Term,
                predicate.MaxEdits,
                predicate.PrefixLength,
                predicate.Transpositions))
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

    private static RowGroupPredicateDecision? TryEvaluateBitmap(
        ParquetRowGroupPlannerContext context,
        PushdownPredicate<DynamicRow> predicate,
        ComparisonPushdownPredicate<DynamicRow> comparison)
    {
        var metadataKey = BitmapMetadataPrefix + Uri.EscapeDataString(predicate.ColumnPath);
        if (!context.Reader.CustomMetadata.TryGetValue(metadataKey, out var payload))
            return null;

        var index = ParquetFooterMetadata.TryDeserialize<BitmapIndexModel>(payload);
        if (index is null)
            return null;

        if (!TryFormatValue(comparison.Value, out var value))
            return null;

        var entry = index.Values.FirstOrDefault(
            candidate => string.Equals(candidate.Value, value, StringComparison.Ordinal));
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

    private static bool TryFormatValue(object? value, out string formatted)
    {
        switch (value)
        {
            case null:
                formatted = string.Empty;
                return false;
            case string text:
                formatted = text;
                return true;
            case Guid guid:
                formatted = guid.ToString("D");
                return true;
            case DateTime dateTime:
                formatted = dateTime.ToUniversalTime().ToString("O", CultureInfo.InvariantCulture);
                return true;
            case DateTimeOffset dateTimeOffset:
                formatted = dateTimeOffset.ToUniversalTime().ToString("O", CultureInfo.InvariantCulture);
                return true;
            case TimeSpan timeSpan:
                formatted = timeSpan.ToString("c", CultureInfo.InvariantCulture);
                return true;
            case byte[] bytes:
                formatted = Convert.ToBase64String(bytes);
                return true;
        }

        if (value is IFormattable formattable)
        {
            formatted = formattable.ToString(null, CultureInfo.InvariantCulture) ?? string.Empty;
            return true;
        }

        formatted = string.Empty;
        return false;
    }

    internal sealed class LuceneIndexModel
    {
        public string ColumnPath { get; set; } = string.Empty;
        public List<string> Terms { get; set; } = new();
        public List<int[]> RowGroups { get; set; } = new();
    }

    internal sealed class BitmapIndexModel
    {
        public string ColumnPath { get; set; } = string.Empty;
        public List<BitmapEntryModel> Values { get; set; } = new();
    }

    internal sealed class BitmapEntryModel
    {
        public string Value { get; set; } = string.Empty;
        public int[] RowGroups { get; set; } = Array.Empty<int>();
    }
}
