using System.IO.Compression;
using System.Text;
using System.Text.Json;
using Parquet;
using Parquet.Data;
using Parquet.Query.Viewer.Models;
using Parquet.Schema;

namespace Parquet.Query.Viewer.Services;

public sealed class PredicateEvaluator
{
    private const string LuceneMetadataPrefix = "parquet.query.lucene.v1/";
    private static readonly JsonSerializerOptions s_jsonOptions = new(JsonSerializerDefaults.Web);

    public (bool ShouldRead, string Reason) EvaluateRowGroup(
        string filePath,
        IReadOnlyDictionary<string, string> customMetadata,
        int rowGroupIndex,
        IParquetRowGroupReader rgReader,
        DataField[] dataFields,
        QueryPredicate[] predicates)
    {
        if (predicates.Length == 0)
        {
            return (true, "No predicates - read all row groups.");
        }

        var reasons = new List<string>();
        foreach (var predicate in predicates)
        {
            var field = dataFields.FirstOrDefault(f => f.Name.Equals(predicate.Column, StringComparison.OrdinalIgnoreCase));
            if (field is null)
            {
                reasons.Add($"Column '{predicate.Column}' not found in schema.");
                continue;
            }

            if (!rgReader.ColumnExists(field))
            {
                reasons.Add($"Column '{predicate.Column}' not present in row group.");
                continue;
            }

            if (IsLucenePredicate(predicate.Operator))
            {
                var (canSkip, reason) = EvaluateAgainstLuceneIndex(filePath, customMetadata, rowGroupIndex, predicate, field);
                if (canSkip)
                {
                    return (false, reason);
                }

                reasons.Add(reason);
                continue;
            }

            var stats = rgReader.GetStatistics(field);
            if (stats is null)
            {
                reasons.Add($"{predicate.Column}: no statistics available - must read.");
                continue;
            }

            var (statisticsCanSkip, reasonFromStatistics) = EvaluateAgainstStatistics(predicate, field, stats);
            if (statisticsCanSkip)
            {
                return (false, reasonFromStatistics);
            }

            reasons.Add(reasonFromStatistics);
        }

        return (true, string.Join(" | ", reasons));
    }

    public bool MatchesAllPredicates(
        Dictionary<string, object?> rowValues,
        DataField[] dataFields,
        QueryPredicate[] predicates)
    {
        foreach (var predicate in predicates)
        {
            if (!rowValues.TryGetValue(predicate.Column, out var value))
            {
                return false;
            }

            if (!MatchesPredicate(value, predicate))
            {
                return false;
            }
        }

        return true;
    }

    private static (bool CanSkip, string Reason) EvaluateAgainstStatistics(
        QueryPredicate predicate,
        DataField field,
        DataColumnStatistics stats)
    {
        var parsedValue = ParseValue(predicate.Value, field);
        if (parsedValue is null && predicate.Operator != "==" && predicate.Operator != "!=")
        {
            return (false, $"{predicate.Column}: could not parse value '{predicate.Value}' - must read.");
        }

        switch (predicate.Operator)
        {
            case "==":
                if (parsedValue is not null &&
                    stats.MinValue is not null && CompareValues(parsedValue, stats.MinValue) < 0)
                {
                    return (true, $"{predicate.Column}: {predicate.Value} < min({FormatStat(stats.MinValue)}) - SKIP");
                }

                if (parsedValue is not null &&
                    stats.MaxValue is not null && CompareValues(parsedValue, stats.MaxValue) > 0)
                {
                    return (true, $"{predicate.Column}: {predicate.Value} > max({FormatStat(stats.MaxValue)}) - SKIP");
                }

                return (false, $"{predicate.Column}: {predicate.Value} in [{FormatStat(stats.MinValue)}..{FormatStat(stats.MaxValue)}] - READ");

            case "!=":
                if (parsedValue is not null &&
                    stats.MinValue is not null && stats.MaxValue is not null &&
                    CompareValues(parsedValue, stats.MinValue) == 0 &&
                    CompareValues(parsedValue, stats.MaxValue) == 0 &&
                    (!stats.NullCount.HasValue || stats.NullCount.Value == 0))
                {
                    return (true, $"{predicate.Column}: all values = {predicate.Value} - SKIP");
                }

                return (false, $"{predicate.Column}: != {predicate.Value}, range [{FormatStat(stats.MinValue)}..{FormatStat(stats.MaxValue)}] - READ");

            case ">":
                if (stats.MaxValue is not null && CompareValues(stats.MaxValue, parsedValue) <= 0)
                {
                    return (true, $"{predicate.Column}: max({FormatStat(stats.MaxValue)}) <= {predicate.Value} - SKIP");
                }

                return (false, $"{predicate.Column}: max({FormatStat(stats.MaxValue)}) > {predicate.Value} - READ");

            case ">=":
                if (stats.MaxValue is not null && CompareValues(stats.MaxValue, parsedValue) < 0)
                {
                    return (true, $"{predicate.Column}: max({FormatStat(stats.MaxValue)}) < {predicate.Value} - SKIP");
                }

                return (false, $"{predicate.Column}: max({FormatStat(stats.MaxValue)}) >= {predicate.Value} - READ");

            case "<":
                if (stats.MinValue is not null && CompareValues(stats.MinValue, parsedValue) >= 0)
                {
                    return (true, $"{predicate.Column}: min({FormatStat(stats.MinValue)}) >= {predicate.Value} - SKIP");
                }

                return (false, $"{predicate.Column}: min({FormatStat(stats.MinValue)}) < {predicate.Value} - READ");

            case "<=":
                if (stats.MinValue is not null && CompareValues(stats.MinValue, parsedValue) > 0)
                {
                    return (true, $"{predicate.Column}: min({FormatStat(stats.MinValue)}) > {predicate.Value} - SKIP");
                }

                return (false, $"{predicate.Column}: min({FormatStat(stats.MinValue)}) <= {predicate.Value} - READ");

            case "Between":
                var upper = predicate.Value2 is not null ? ParseValue(predicate.Value2, field) : null;
                if (parsedValue is not null && stats.MinValue is not null && CompareValues(stats.MinValue, upper) > 0)
                {
                    return (true, $"{predicate.Column}: min({FormatStat(stats.MinValue)}) > {predicate.Value2} - SKIP");
                }

                if (upper is not null && stats.MaxValue is not null && CompareValues(stats.MaxValue, parsedValue) < 0)
                {
                    return (true, $"{predicate.Column}: max({FormatStat(stats.MaxValue)}) < {predicate.Value} - SKIP");
                }

                return (false, $"{predicate.Column}: range overlaps [{predicate.Value}..{predicate.Value2}] - READ");

            case "StartsWith":
                if (stats.MinValue is string && stats.MaxValue is string maxStr &&
                    string.CompareOrdinal(maxStr, predicate.Value) < 0)
                {
                    return (true, $"{predicate.Column}: max('{maxStr}') < prefix '{predicate.Value}' - SKIP");
                }

                return (false, $"{predicate.Column}: StartsWith '{predicate.Value}' - READ");

            case "EndsWith":
                return (false, $"{predicate.Column}: EndsWith '{predicate.Value}' - residual scan only.");

            case "Contains":
                return (false, $"{predicate.Column}: Contains '{predicate.Value}' - residual scan only.");

            case "IsNull":
                if (stats.NullCount.HasValue && stats.NullCount.Value == 0)
                {
                    return (true, $"{predicate.Column}: nullCount=0 - SKIP (no nulls)");
                }

                return (false, $"{predicate.Column}: IsNull - READ");

            case "IsNotNull":
                return (false, $"{predicate.Column}: IsNotNull - READ (requires row scan)");

            default:
                return (false, $"{predicate.Column}: unknown operator '{predicate.Operator}' - must read.");
        }
    }

    private static bool MatchesPredicate(object? value, QueryPredicate predicate)
    {
        if (predicate.Operator == "IsNull")
        {
            return value is null;
        }

        if (predicate.Operator == "IsNotNull")
        {
            return value is not null;
        }

        if (value is null)
        {
            return predicate.Operator == "==" && predicate.Value == "";
        }

        var strValue = value.ToString() ?? "";
        return predicate.Operator switch
        {
            "==" => string.Equals(strValue, predicate.Value, StringComparison.OrdinalIgnoreCase),
            "!=" => !string.Equals(strValue, predicate.Value, StringComparison.OrdinalIgnoreCase),
            ">" => CompareStrings(strValue, predicate.Value) > 0,
            ">=" => CompareStrings(strValue, predicate.Value) >= 0,
            "<" => CompareStrings(strValue, predicate.Value) < 0,
            "<=" => CompareStrings(strValue, predicate.Value) <= 0,
            "Between" => CompareStrings(strValue, predicate.Value) >= 0 &&
                         (predicate.Value2 is null || CompareStrings(strValue, predicate.Value2) <= 0),
            "StartsWith" => strValue.StartsWith(predicate.Value, StringComparison.OrdinalIgnoreCase),
            "EndsWith" => strValue.EndsWith(predicate.Value, StringComparison.OrdinalIgnoreCase),
            "Contains" => strValue.Contains(predicate.Value, StringComparison.OrdinalIgnoreCase),
            "LuceneMatch" => MatchesLucenePredicate(strValue, predicate),
            "LuceneFuzzy" => MatchesLucenePredicate(strValue, predicate),
            _ => true
        };
    }

    private static (bool CanSkip, string Reason) EvaluateAgainstLuceneIndex(
        string filePath,
        IReadOnlyDictionary<string, string> customMetadata,
        int rowGroupIndex,
        QueryPredicate predicate,
        DataField field)
    {
        if (field.ClrType != typeof(string))
        {
            return (false, $"{predicate.Column}: Lucene predicates support string columns only - READ.");
        }

        var metadataKey = LuceneMetadataPrefix + Uri.EscapeDataString(field.Path.ToString());
        if (!customMetadata.TryGetValue(metadataKey, out var payload))
        {
            return (false, $"{predicate.Column}: no Lucene footer index - READ (residual verification).");
        }

        var index = TryDeserializeLuceneFooterIndex(payload);
        if (index is null || rowGroupIndex >= index.RowGroups.Count)
        {
            return (false, $"{predicate.Column}: Lucene footer index unavailable - READ (residual verification).");
        }

        var normalizedTerm = AnalyzeSingleTerm(predicate.Value);
        var maxEdits = GetLuceneMaxEdits(predicate);
        var prefixLength = GetLucenePrefixLength(predicate);
        var transpositions = GetLuceneTranspositions(predicate);
        var matchingTerms = index.RowGroups[rowGroupIndex]
            .Where(ordinal => ordinal >= 0 && ordinal < index.Terms.Count)
            .Select(ordinal => index.Terms[ordinal])
            .Where(candidate => IsLuceneTermMatch(candidate, normalizedTerm, maxEdits, prefixLength, transpositions))
            .Take(8)
            .ToArray();

        if (matchingTerms.Length == 0)
        {
            return (true, $"{predicate.Column}: Lucene index ruled out '{normalizedTerm}' - SKIP");
        }

        return (false, $"{predicate.Column}: Lucene terms matched {string.Join(", ", matchingTerms)} - READ");
    }

    private static bool MatchesLucenePredicate(string value, QueryPredicate predicate)
    {
        var normalizedTerm = AnalyzeSingleTerm(predicate.Value);
        var maxEdits = GetLuceneMaxEdits(predicate);
        var prefixLength = GetLucenePrefixLength(predicate);
        var transpositions = GetLuceneTranspositions(predicate);

        return Analyze(value).Any(token => IsLuceneTermMatch(token, normalizedTerm, maxEdits, prefixLength, transpositions));
    }

    private static bool IsLucenePredicate(string @operator) =>
        @operator is "LuceneMatch" or "LuceneFuzzy";

    private static int GetLuceneMaxEdits(QueryPredicate predicate) =>
        predicate.Operator == "LuceneMatch" ? 0 : predicate.MaxEdits ?? 1;

    private static int GetLucenePrefixLength(QueryPredicate predicate) =>
        predicate.PrefixLength ?? 0;

    private static bool GetLuceneTranspositions(QueryPredicate predicate) =>
        predicate.Transpositions ?? true;

    private static IReadOnlyList<string> Analyze(string? value)
    {
        if (string.IsNullOrWhiteSpace(value))
        {
            return Array.Empty<string>();
        }

        var tokens = new List<string>();
        var buffer = new StringBuilder(value.Length);
        foreach (var character in value)
        {
            if (char.IsLetterOrDigit(character))
            {
                buffer.Append(char.ToLowerInvariant(character));
                continue;
            }

            FlushToken(buffer, tokens);
        }

        FlushToken(buffer, tokens);
        return tokens;
    }

    private static string AnalyzeSingleTerm(string term)
    {
        if (string.IsNullOrWhiteSpace(term))
        {
            throw new ArgumentException("Value cannot be null, empty, or whitespace.", nameof(term));
        }

        var tokens = Analyze(term);
        if (tokens.Count != 1)
        {
            throw new ArgumentException(
                $"Lucene term queries require exactly one analyzed token. Received '{term}'.",
                nameof(term));
        }

        return tokens[0];
    }

    private static void FlushToken(StringBuilder buffer, ICollection<string> tokens)
    {
        if (buffer.Length == 0)
        {
            return;
        }

        tokens.Add(buffer.ToString());
        buffer.Clear();
    }

    private static bool IsLuceneTermMatch(
        string candidate,
        string query,
        int maxEdits,
        int prefixLength,
        bool transpositions)
    {
        if (maxEdits < 0 || maxEdits > 2)
        {
            throw new ArgumentOutOfRangeException(nameof(maxEdits), "Lucene-style fuzzy matching supports edit distances from 0 to 2.");
        }

        if (prefixLength < 0)
        {
            throw new ArgumentOutOfRangeException(nameof(prefixLength));
        }

        if (prefixLength > candidate.Length || prefixLength > query.Length)
        {
            return false;
        }

        if (!candidate.AsSpan(0, prefixLength).SequenceEqual(query.AsSpan(0, prefixLength)))
        {
            return false;
        }

        if (maxEdits == 0)
        {
            return string.Equals(candidate, query, StringComparison.Ordinal);
        }

        var candidateSuffix = candidate.Substring(prefixLength);
        var querySuffix = query.Substring(prefixLength);
        if (Math.Abs(candidateSuffix.Length - querySuffix.Length) > maxEdits)
        {
            return false;
        }

        return transpositions
            ? DamerauLevenshteinDistance(candidateSuffix, querySuffix, maxEdits) <= maxEdits
            : LevenshteinDistance(candidateSuffix, querySuffix, maxEdits) <= maxEdits;
    }

    private static int LevenshteinDistance(string left, string right, int maxEdits)
    {
        var previous = new int[right.Length + 1];
        var current = new int[right.Length + 1];

        for (var column = 0; column <= right.Length; column++)
        {
            previous[column] = column;
        }

        for (var row = 1; row <= left.Length; row++)
        {
            current[0] = row;
            var bestInRow = current[0];

            for (var column = 1; column <= right.Length; column++)
            {
                var substitutionCost = left[row - 1] == right[column - 1] ? 0 : 1;
                current[column] = Math.Min(
                    Math.Min(previous[column] + 1, current[column - 1] + 1),
                    previous[column - 1] + substitutionCost);
                bestInRow = Math.Min(bestInRow, current[column]);
            }

            if (bestInRow > maxEdits)
            {
                return bestInRow;
            }

            (previous, current) = (current, previous);
        }

        return previous[right.Length];
    }

    private static int DamerauLevenshteinDistance(string left, string right, int maxEdits)
    {
        var matrix = new int[left.Length + 1, right.Length + 1];

        for (var row = 0; row <= left.Length; row++)
        {
            matrix[row, 0] = row;
        }

        for (var column = 0; column <= right.Length; column++)
        {
            matrix[0, column] = column;
        }

        for (var row = 1; row <= left.Length; row++)
        {
            var bestInRow = int.MaxValue;

            for (var column = 1; column <= right.Length; column++)
            {
                var substitutionCost = left[row - 1] == right[column - 1] ? 0 : 1;
                var value = Math.Min(
                    Math.Min(matrix[row - 1, column] + 1, matrix[row, column - 1] + 1),
                    matrix[row - 1, column - 1] + substitutionCost);

                if (row > 1 &&
                    column > 1 &&
                    left[row - 1] == right[column - 2] &&
                    left[row - 2] == right[column - 1])
                {
                    value = Math.Min(value, matrix[row - 2, column - 2] + 1);
                }

                matrix[row, column] = value;
                bestInRow = Math.Min(bestInRow, value);
            }

            if (bestInRow > maxEdits)
            {
                return bestInRow;
            }
        }

        return matrix[left.Length, right.Length];
    }

    private static ViewerLuceneFooterIndexModel? TryDeserializeLuceneFooterIndex(string? payload)
    {
        if (string.IsNullOrWhiteSpace(payload))
        {
            return null;
        }

        try
        {
            var compressedBytes = Convert.FromBase64String(payload);
            using var input = new MemoryStream(compressedBytes, writable: false);
            using var compression = new BrotliStream(input, CompressionMode.Decompress);
            using var json = new MemoryStream();
            compression.CopyTo(json);
            return JsonSerializer.Deserialize<ViewerLuceneFooterIndexModel>(json.ToArray(), s_jsonOptions);
        }
        catch (Exception ex) when (ex is FormatException or JsonException or InvalidDataException or InvalidOperationException)
        {
            return null;
        }
    }

    private static int CompareStrings(string a, string b)
    {
        if (double.TryParse(a, out var da) && double.TryParse(b, out var db))
        {
            return da.CompareTo(db);
        }

        return string.Compare(a, b, StringComparison.OrdinalIgnoreCase);
    }

    private static object? ParseValue(string value, DataField field)
    {
        try
        {
            var type = Nullable.GetUnderlyingType(field.ClrType) ?? field.ClrType;
            if (type == typeof(string)) return value;
            if (type == typeof(int)) return int.Parse(value);
            if (type == typeof(long)) return long.Parse(value);
            if (type == typeof(float)) return float.Parse(value);
            if (type == typeof(double)) return double.Parse(value);
            if (type == typeof(decimal)) return decimal.Parse(value);
            if (type == typeof(bool)) return bool.Parse(value);
            if (type == typeof(DateTime)) return DateTime.Parse(value);
            if (type == typeof(DateTimeOffset)) return DateTimeOffset.Parse(value);
            if (type == typeof(short)) return short.Parse(value);
            if (type == typeof(byte)) return byte.Parse(value);
            return value;
        }
        catch
        {
            return null;
        }
    }

    private static int CompareValues(object? left, object? right)
    {
        if (left is null && right is null) return 0;
        if (left is null) return -1;
        if (right is null) return 1;

        try
        {
            if (left is IComparable comparable)
            {
                if (left.GetType() == right.GetType())
                {
                    return comparable.CompareTo(right);
                }

                if (double.TryParse(left.ToString(), out var ld) && double.TryParse(right.ToString(), out var rd))
                {
                    return ld.CompareTo(rd);
                }

                return string.Compare(left.ToString(), right.ToString(), StringComparison.OrdinalIgnoreCase);
            }
        }
        catch
        {
        }

        return string.Compare(left.ToString(), right.ToString(), StringComparison.OrdinalIgnoreCase);
    }

    private static string FormatStat(object? value) => value?.ToString() ?? "null";

    private sealed class ViewerLuceneFooterIndexModel
    {
        public string ColumnPath { get; set; } = string.Empty;

        public List<string> Terms { get; set; } = [];

        public List<int[]> RowGroups { get; set; } = [];
    }
}
