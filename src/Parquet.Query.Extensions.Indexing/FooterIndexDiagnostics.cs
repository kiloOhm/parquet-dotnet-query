namespace Parquet.Query.Extensions.Indexing;

internal static class FooterIndexDiagnostics
{
    public const int RecommendedBitmapDistinctValueThreshold = 256;

    public static void WarnBitmapUnsupportedType(string columnPath, Type fieldType)
    {
        WriteWarning(
            "footer-bitmap",
            columnPath,
            $"Column type '{fieldType.Name}' cannot be normalized into bitmap values.",
            GetUnsupportedTypeSuggestion(fieldType));
    }

    public static void WarnBitmapHighCardinality(string columnPath, Type fieldType, int distinctValueCount, int maxDistinctValues)
    {
        WriteWarning(
            "footer-bitmap",
            columnPath,
            $"Column type '{fieldType.Name}' reached {distinctValueCount} distinct values, exceeding the configured bitmap limit of {maxDistinctValues}.",
            GetHighCardinalitySuggestion(fieldType));
    }

    private static void WriteWarning(string indexName, string columnPath, string reason, string suggestion)
    {
        Console.Error.WriteLine(
            $"[Parquet.Query] Warning: The {indexName} index on column '{columnPath}' may be a poor fit. {reason} {suggestion}");
    }

    private static string GetLowCardinalitySuggestion(Type fieldType)
    {
        var parts = new List<string>
        {
            $"Consider a footer bitmap index for low-cardinality equality pruning (roughly <= {RecommendedBitmapDistinctValueThreshold} distinct values)"
        };

        if (fieldType == typeof(string))
        {
            parts.Add("and `lucene` if the column is searched as free text instead of exact equality");
        }
        else if (IsOrderedScalar(fieldType))
        {
            parts.Add("or a sort key if range predicates are more common than exact matches");
        }

        return string.Join(" ", parts) + ".";
    }

    private static string GetHighCardinalitySuggestion(Type fieldType)
    {
        var parts = new List<string>
        {
            "Consider a bloom filter for high-cardinality equality lookups"
        };

        if (fieldType == typeof(string))
        {
            parts.Add("and `lucene` if you need tokenized or fuzzy text matching");
        }
        else if (IsOrderedScalar(fieldType))
        {
            parts.Add("or a sort key if the column is mainly filtered by ranges");
        }

        return string.Join(" ", parts) + ".";
    }

    private static string GetUnsupportedTypeSuggestion(Type fieldType)
    {
        if (fieldType == typeof(string))
        {
            return "Consider `lucene` for text search, or a custom external index if you need a different matching strategy.";
        }

        if (IsOrderedScalar(fieldType))
        {
            return "Consider sort keys or parquet statistics/page indexes for range filters, or a bloom filter for equality-heavy lookups.";
        }

        return "Consider a bloom filter, parquet sort/statistics metadata, or a custom external index tailored to the column shape.";
    }

    private static bool IsOrderedScalar(Type fieldType)
    {
        var unwrapped = Nullable.GetUnderlyingType(fieldType) ?? fieldType;
        if (unwrapped.IsEnum)
        {
            return true;
        }

        return unwrapped == typeof(byte) ||
            unwrapped == typeof(sbyte) ||
            unwrapped == typeof(short) ||
            unwrapped == typeof(ushort) ||
            unwrapped == typeof(int) ||
            unwrapped == typeof(uint) ||
            unwrapped == typeof(long) ||
            unwrapped == typeof(ulong) ||
            unwrapped == typeof(float) ||
            unwrapped == typeof(double) ||
            unwrapped == typeof(decimal) ||
            unwrapped == typeof(DateTime) ||
            unwrapped == typeof(DateTimeOffset) ||
            unwrapped == typeof(TimeSpan);
    }
}
