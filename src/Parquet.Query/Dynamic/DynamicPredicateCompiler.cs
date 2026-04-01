using System.Globalization;
using System.Text;
using Parquet.Query.Pushdown;
using Parquet.Schema;

namespace Parquet.Query.Dynamic;

/// <summary>
/// Compiles <see cref="DynamicPredicate"/> instances into a <see cref="PushdownFilter{T}"/>
/// over <see cref="DynamicRow"/> and a set of residual predicates for operators that cannot
/// participate in pushdown planning.
/// </summary>
internal static class DynamicPredicateCompiler
{
    public static DynamicFilterPlan Compile(
        IReadOnlyList<DynamicPredicate> predicates,
        IReadOnlyDictionary<string, DataField> dataFields)
    {
        var pushdown = new List<PushdownPredicate<DynamicRow>>();
        var residual = new List<Func<DynamicRow, bool>>();

        foreach (var predicate in predicates)
        {
            if (!dataFields.TryGetValue(predicate.Column, out var field))
            {
                continue;
            }

            var targetType = Nullable.GetUnderlyingType(field.ClrNullableIfHasNullsType) ?? field.ClrType;
            var op = NormalizeOperator(predicate.Operator);

            switch (op)
            {
                case "==":
                    AddComparison(pushdown, predicate.Column, ComparisonOperator.Equal, predicate.Value, targetType);
                    break;

                case "!=":
                    AddComparison(pushdown, predicate.Column, ComparisonOperator.NotEqual, predicate.Value, targetType);
                    break;

                case "<":
                    AddComparison(pushdown, predicate.Column, ComparisonOperator.LessThan, predicate.Value, targetType);
                    break;

                case "<=":
                    AddComparison(pushdown, predicate.Column, ComparisonOperator.LessThanOrEqual, predicate.Value, targetType);
                    break;

                case ">":
                    AddComparison(pushdown, predicate.Column, ComparisonOperator.GreaterThan, predicate.Value, targetType);
                    break;

                case ">=":
                    AddComparison(pushdown, predicate.Column, ComparisonOperator.GreaterThanOrEqual, predicate.Value, targetType);
                    break;

                case "between":
                    AddComparison(pushdown, predicate.Column, ComparisonOperator.GreaterThanOrEqual, predicate.Value, targetType);
                    AddComparison(pushdown, predicate.Column, ComparisonOperator.LessThanOrEqual, predicate.Value2, targetType);
                    break;

                case "startswith":
                    pushdown.Add(CreateStartsWith(predicate.Column, predicate.Value ?? string.Empty));
                    break;

                case "endswith":
                {
                    var suffix = predicate.Value ?? string.Empty;
                    var col = predicate.Column;
                    residual.Add(row =>
                        row.Values.TryGetValue(col, out var v) &&
                        v is string s &&
                        s.EndsWith(suffix, StringComparison.Ordinal));
                    break;
                }

                case "contains":
                {
                    var substring = predicate.Value ?? string.Empty;
                    var col = predicate.Column;
                    residual.Add(row =>
                        row.Values.TryGetValue(col, out var v) &&
                        v is string s &&
                        s.IndexOf(substring, StringComparison.Ordinal) >= 0);
                    break;
                }

                case "isnull":
                {
                    var col = predicate.Column;
                    residual.Add(row =>
                        !row.Values.TryGetValue(col, out var v) || v is null);
                    break;
                }

                case "isnotnull":
                {
                    var col = predicate.Column;
                    residual.Add(row =>
                        row.Values.TryGetValue(col, out var v) && v is not null);
                    break;
                }

                case "lucenematch":
                case "lucenefuzzy":
                {
                    var col = predicate.Column;
                    var term = AnalyzeSingleTerm(predicate.Value);
                    var maxEdits = predicate.MaxEdits ?? (op == "lucenefuzzy" ? 1 : 0);
                    var prefixLen = predicate.PrefixLength ?? 0;
                    var transpositions = predicate.Transpositions ?? true;
                    var description = maxEdits == 0
                        ? $"lucene.match({col}, \"{predicate.Value}\")"
                        : $"lucene.fuzzy({col}, \"{predicate.Value}\", maxEdits: {maxEdits}, prefixLength: {prefixLen}, transpositions: {transpositions.ToString().ToLowerInvariant()})";
                    Func<DynamicRow, bool> rowPredicate = row =>
                        row.Values.TryGetValue(col, out var v) &&
                        v is string s &&
                        Analyze(s).Any(token => IsTermMatch(token, term, maxEdits, prefixLen, transpositions));
                    pushdown.Add(new DynamicLuceneTermPredicate(
                        col, term, maxEdits, prefixLen, transpositions, description, rowPredicate));
                    break;
                }

                default:
                    throw new NotSupportedException($"Operator '{predicate.Operator}' is not supported.");
            }
        }

        return new DynamicFilterPlan(
            new PushdownFilter<DynamicRow>(pushdown),
            residual);
    }

    private static void AddComparison(
        List<PushdownPredicate<DynamicRow>> target,
        string columnPath,
        ComparisonOperator op,
        string? rawValue,
        Type targetType)
    {
        var value = ParseValue(rawValue, targetType);
        target.Add(CreateComparison(columnPath, op, value, targetType));
    }

    private static ComparisonPushdownPredicate<DynamicRow> CreateComparison(
        string columnPath,
        ComparisonOperator op,
        object? value,
        Type valueType)
    {
        var description = $"{columnPath} {ToSymbol(op)} {PushdownPredicateFactory.FormatValue(value)}";

        Func<DynamicRow, bool> rowPredicate = row =>
        {
            if (!row.Values.TryGetValue(columnPath, out var actual))
                return false;

            if (actual is null && value is null)
                return op == ComparisonOperator.Equal;

            if (actual is null || value is null)
                return op == ComparisonOperator.NotEqual;

            if (actual is IComparable comparable)
            {
                var converted = actual.GetType() == value.GetType()
                    ? value
                    : PushdownPredicateFactory.ConvertValue(value, actual.GetType());
                var cmp = comparable.CompareTo(converted);
                return op switch
                {
                    ComparisonOperator.Equal => cmp == 0,
                    ComparisonOperator.NotEqual => cmp != 0,
                    ComparisonOperator.LessThan => cmp < 0,
                    ComparisonOperator.LessThanOrEqual => cmp <= 0,
                    ComparisonOperator.GreaterThan => cmp > 0,
                    ComparisonOperator.GreaterThanOrEqual => cmp >= 0,
                    _ => false
                };
            }

            return op == ComparisonOperator.Equal && Equals(actual, value);
        };

        return new ComparisonPushdownPredicate<DynamicRow>(
            columnPath, columnPath, op, value, valueType, description, rowPredicate);
    }

    private static StartsWithPushdownPredicate<DynamicRow> CreateStartsWith(
        string columnPath,
        string prefix)
    {
        var description = $"{columnPath}.StartsWith({PushdownPredicateFactory.FormatValue(prefix)})";

        Func<DynamicRow, bool> rowPredicate = row =>
            row.Values.TryGetValue(columnPath, out var v) &&
            v is string s &&
            s.StartsWith(prefix, StringComparison.Ordinal);

        return new StartsWithPushdownPredicate<DynamicRow>(
            columnPath, columnPath, prefix, description, rowPredicate);
    }

    private static object? ParseValue(string? text, Type targetType)
    {
        if (text is null)
            return null;

        if (targetType == typeof(DateTimeOffset))
            return DateTimeOffset.Parse(text, CultureInfo.InvariantCulture);

        if (targetType == typeof(TimeSpan))
            return TimeSpan.Parse(text, CultureInfo.InvariantCulture);

        if (targetType == typeof(Guid))
            return Guid.Parse(text);

        if (targetType == typeof(byte[]))
            return Convert.FromBase64String(text);

        return PushdownPredicateFactory.ConvertValue(text, targetType);
    }

    private static string NormalizeOperator(string op)
    {
        switch (op.Trim().ToLowerInvariant())
        {
            case "=":
            case "==":
            case "eq":
                return "==";
            case "!=":
            case "<>":
            case "ne":
                return "!=";
            case "<":
            case "lt":
                return "<";
            case "<=":
            case "le":
                return "<=";
            case ">":
            case "gt":
                return ">";
            case ">=":
            case "ge":
                return ">=";
            case "between":
                return "between";
            case "startswith":
                return "startswith";
            case "endswith":
                return "endswith";
            case "contains":
                return "contains";
            case "isnull":
                return "isnull";
            case "isnotnull":
                return "isnotnull";
            case "lucenematch":
                return "lucenematch";
            case "lucenefuzzy":
                return "lucenefuzzy";
            default:
                return op;
        }
    }

    private static string ToSymbol(ComparisonOperator op) =>
        op switch
        {
            ComparisonOperator.Equal => "==",
            ComparisonOperator.NotEqual => "!=",
            ComparisonOperator.LessThan => "<",
            ComparisonOperator.LessThanOrEqual => "<=",
            ComparisonOperator.GreaterThan => ">",
            ComparisonOperator.GreaterThanOrEqual => ">=",
            _ => op.ToString()
        };

    private static IReadOnlyList<string> Analyze(string? value)
    {
        if (string.IsNullOrWhiteSpace(value))
            return Array.Empty<string>();

        var tokens = new List<string>();
        var buffer = new StringBuilder(value.Length);
        foreach (var c in value)
        {
            if (char.IsLetterOrDigit(c))
            {
                buffer.Append(char.ToLowerInvariant(c));
                continue;
            }

            if (buffer.Length > 0)
            {
                tokens.Add(buffer.ToString());
                buffer.Clear();
            }
        }

        if (buffer.Length > 0)
            tokens.Add(buffer.ToString());

        return tokens;
    }

    private static string AnalyzeSingleTerm(string? term)
    {
        var tokens = Analyze(term);
        return tokens.Count == 1 ? tokens[0] : term?.ToLowerInvariant() ?? string.Empty;
    }

    internal static bool IsTermMatch(string candidate, string query, int maxEdits, int prefixLength, bool transpositions)
    {
        if (prefixLength > candidate.Length || prefixLength > query.Length)
            return false;

        if (prefixLength > 0 &&
            string.CompareOrdinal(candidate, 0, query, 0, prefixLength) != 0)
            return false;

        if (maxEdits == 0)
            return string.Equals(candidate, query, StringComparison.Ordinal);

        var candidateSuffix = candidate.Substring(prefixLength);
        var querySuffix = query.Substring(prefixLength);
        if (Math.Abs(candidateSuffix.Length - querySuffix.Length) > maxEdits)
            return false;

        var distance = transpositions
            ? DamerauLevenshteinDistance(candidateSuffix, querySuffix, maxEdits)
            : LevenshteinDistance(candidateSuffix, querySuffix, maxEdits);
        return distance <= maxEdits;
    }

    private static int LevenshteinDistance(string left, string right, int maxEdits)
    {
        var previous = new int[right.Length + 1];
        var current = new int[right.Length + 1];
        for (var col = 0; col <= right.Length; col++)
            previous[col] = col;

        for (var row = 1; row <= left.Length; row++)
        {
            current[0] = row;
            var best = current[0];
            for (var col = 1; col <= right.Length; col++)
            {
                var cost = left[row - 1] == right[col - 1] ? 0 : 1;
                current[col] = Math.Min(Math.Min(previous[col] + 1, current[col - 1] + 1), previous[col - 1] + cost);
                best = Math.Min(best, current[col]);
            }

            if (best > maxEdits) return best;
            (previous, current) = (current, previous);
        }

        return previous[right.Length];
    }

    private static int DamerauLevenshteinDistance(string left, string right, int maxEdits)
    {
        var matrix = new int[left.Length + 1, right.Length + 1];
        for (var row = 0; row <= left.Length; row++) matrix[row, 0] = row;
        for (var col = 0; col <= right.Length; col++) matrix[0, col] = col;

        for (var row = 1; row <= left.Length; row++)
        {
            var best = int.MaxValue;
            for (var col = 1; col <= right.Length; col++)
            {
                var cost = left[row - 1] == right[col - 1] ? 0 : 1;
                var val = Math.Min(Math.Min(matrix[row - 1, col] + 1, matrix[row, col - 1] + 1), matrix[row - 1, col - 1] + cost);
                if (row > 1 && col > 1 && left[row - 1] == right[col - 2] && left[row - 2] == right[col - 1])
                    val = Math.Min(val, matrix[row - 2, col - 2] + 1);
                matrix[row, col] = val;
                best = Math.Min(best, val);
            }

            if (best > maxEdits) return best;
        }

        return matrix[left.Length, right.Length];
    }
}

internal sealed class DynamicFilterPlan
{
    public DynamicFilterPlan(
        PushdownFilter<DynamicRow> pushdownFilter,
        IReadOnlyList<Func<DynamicRow, bool>> residualPredicates)
    {
        PushdownFilter = pushdownFilter;
        ResidualPredicates = residualPredicates;
    }

    public PushdownFilter<DynamicRow> PushdownFilter { get; }

    public IReadOnlyList<Func<DynamicRow, bool>> ResidualPredicates { get; }

    public bool MatchesResidual(DynamicRow row) =>
        ResidualPredicates.Count == 0 || ResidualPredicates.All(p => p(row));
}
