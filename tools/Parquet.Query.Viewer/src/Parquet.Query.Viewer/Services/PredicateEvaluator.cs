using Parquet;
using Parquet.Data;
using Parquet.Schema;
using Parquet.Query.Viewer.Models;

namespace Parquet.Query.Viewer.Services;

public sealed class PredicateEvaluator
{
    public (bool ShouldRead, string Reason) EvaluateRowGroup(
        IParquetRowGroupReader rgReader,
        DataField[] dataFields,
        QueryPredicate[] predicates)
    {
        if (predicates.Length == 0)
            return (true, "No predicates — read all row groups.");

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

            var stats = rgReader.GetStatistics(field);
            if (stats is null)
            {
                reasons.Add($"{predicate.Column}: no statistics available — must read.");
                continue;
            }

            var (canSkip, reason) = EvaluateAgainstStatistics(predicate, field, stats);
            if (canSkip)
                return (false, reason);

            reasons.Add(reason);
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
                return false;

            if (!MatchesPredicate(value, predicate))
                return false;
        }
        return true;
    }

    private static (bool CanSkip, string Reason) EvaluateAgainstStatistics(
        QueryPredicate predicate,
        DataField field,
        DataColumnStatistics stats)
    {
        var parsedValue = ParseValue(predicate.Value, field);
        if (parsedValue is null && predicate.Operator != "eq" && predicate.Operator != "neq")
            return (false, $"{predicate.Column}: could not parse value '{predicate.Value}' — must read.");

        switch (predicate.Operator)
        {
            case "eq":
                if (parsedValue is not null &&
                    stats.MinValue is not null && CompareValues(parsedValue, stats.MinValue) < 0)
                    return (true, $"{predicate.Column}: {predicate.Value} < min({FormatStat(stats.MinValue)}) — SKIP");
                if (parsedValue is not null &&
                    stats.MaxValue is not null && CompareValues(parsedValue, stats.MaxValue) > 0)
                    return (true, $"{predicate.Column}: {predicate.Value} > max({FormatStat(stats.MaxValue)}) — SKIP");
                return (false, $"{predicate.Column}: {predicate.Value} in [{FormatStat(stats.MinValue)}..{FormatStat(stats.MaxValue)}] — READ");

            case "neq":
                if (parsedValue is not null &&
                    stats.MinValue is not null && stats.MaxValue is not null &&
                    CompareValues(parsedValue, stats.MinValue) == 0 &&
                    CompareValues(parsedValue, stats.MaxValue) == 0 &&
                    (!stats.NullCount.HasValue || stats.NullCount.Value == 0))
                    return (true, $"{predicate.Column}: all values = {predicate.Value} — SKIP");
                return (false, $"{predicate.Column}: != {predicate.Value}, range [{FormatStat(stats.MinValue)}..{FormatStat(stats.MaxValue)}] — READ");

            case "gt":
                if (stats.MaxValue is not null && CompareValues(stats.MaxValue, parsedValue) <= 0)
                    return (true, $"{predicate.Column}: max({FormatStat(stats.MaxValue)}) <= {predicate.Value} — SKIP");
                return (false, $"{predicate.Column}: max({FormatStat(stats.MaxValue)}) > {predicate.Value} — READ");

            case "ge":
                if (stats.MaxValue is not null && CompareValues(stats.MaxValue, parsedValue) < 0)
                    return (true, $"{predicate.Column}: max({FormatStat(stats.MaxValue)}) < {predicate.Value} — SKIP");
                return (false, $"{predicate.Column}: max({FormatStat(stats.MaxValue)}) >= {predicate.Value} — READ");

            case "lt":
                if (stats.MinValue is not null && CompareValues(stats.MinValue, parsedValue) >= 0)
                    return (true, $"{predicate.Column}: min({FormatStat(stats.MinValue)}) >= {predicate.Value} — SKIP");
                return (false, $"{predicate.Column}: min({FormatStat(stats.MinValue)}) < {predicate.Value} — READ");

            case "le":
                if (stats.MinValue is not null && CompareValues(stats.MinValue, parsedValue) > 0)
                    return (true, $"{predicate.Column}: min({FormatStat(stats.MinValue)}) > {predicate.Value} — SKIP");
                return (false, $"{predicate.Column}: min({FormatStat(stats.MinValue)}) <= {predicate.Value} — READ");

            case "between":
                var upper = predicate.Value2 is not null ? ParseValue(predicate.Value2, field) : null;
                if (parsedValue is not null && stats.MinValue is not null && CompareValues(stats.MinValue, upper) > 0)
                    return (true, $"{predicate.Column}: min({FormatStat(stats.MinValue)}) > {predicate.Value2} — SKIP");
                if (upper is not null && stats.MaxValue is not null && CompareValues(stats.MaxValue, parsedValue) < 0)
                    return (true, $"{predicate.Column}: max({FormatStat(stats.MaxValue)}) < {predicate.Value} — SKIP");
                return (false, $"{predicate.Column}: range overlaps [{predicate.Value}..{predicate.Value2}] — READ");

            case "startsWith":
                if (stats.MinValue is string minStr && stats.MaxValue is string maxStr)
                {
                    if (string.CompareOrdinal(maxStr, predicate.Value) < 0)
                        return (true, $"{predicate.Column}: max('{maxStr}') < prefix '{predicate.Value}' — SKIP");
                }
                return (false, $"{predicate.Column}: startsWith '{predicate.Value}' — READ");

            default:
                return (false, $"{predicate.Column}: unknown operator '{predicate.Operator}' — must read.");
        }
    }

    private static bool MatchesPredicate(object? value, QueryPredicate predicate)
    {
        if (value is null)
            return predicate.Operator == "eq" && predicate.Value == "";

        var strValue = value.ToString() ?? "";
        return predicate.Operator switch
        {
            "eq" => string.Equals(strValue, predicate.Value, StringComparison.OrdinalIgnoreCase),
            "neq" => !string.Equals(strValue, predicate.Value, StringComparison.OrdinalIgnoreCase),
            "gt" => CompareStrings(strValue, predicate.Value) > 0,
            "ge" => CompareStrings(strValue, predicate.Value) >= 0,
            "lt" => CompareStrings(strValue, predicate.Value) < 0,
            "le" => CompareStrings(strValue, predicate.Value) <= 0,
            "between" => CompareStrings(strValue, predicate.Value) >= 0 &&
                         (predicate.Value2 is null || CompareStrings(strValue, predicate.Value2) <= 0),
            "startsWith" => strValue.StartsWith(predicate.Value, StringComparison.OrdinalIgnoreCase),
            _ => true
        };
    }

    private static int CompareStrings(string a, string b)
    {
        if (double.TryParse(a, out var da) && double.TryParse(b, out var db))
            return da.CompareTo(db);
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
                    return comparable.CompareTo(right);

                if (double.TryParse(left.ToString(), out var ld) && double.TryParse(right.ToString(), out var rd))
                    return ld.CompareTo(rd);

                return string.Compare(left.ToString(), right.ToString(), StringComparison.OrdinalIgnoreCase);
            }
        }
        catch { }

        return string.Compare(left.ToString(), right.ToString(), StringComparison.OrdinalIgnoreCase);
    }

    private static string FormatStat(object? value) => value?.ToString() ?? "null";
}
