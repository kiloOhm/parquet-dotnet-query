using System.Text;
using Parquet;
using Parquet.Meta;
using Parquet.Query.Pushdown;
using Parquet.Schema;

namespace Parquet.Query.Planning;

internal static class PagePruner
{
    public static async Task<PagePruningResult> PruneAsync<T>(
        IParquetRowGroupReader rowGroupReader,
        ParquetSchema schema,
        PushdownFilter<T> pushdownFilter,
        CancellationToken cancellationToken)
        where T : class, new()
    {
        if (pushdownFilter.IsEmpty)
        {
            return PagePruningResult.Full(rowGroupReader.RowCount);
        }

        var dataFields = schema.GetDataFields()
            .ToDictionary(field => field.Path.ToString(), StringComparer.Ordinal);

        IReadOnlyList<RowInterval>? survivingIntervals = null;
        var pageCount = 0;
        var pageIndexAvailable = false;
        var usedFallback = false;
        OffsetIndex? lastOffsetIndex = null;

        foreach (var predicate in pushdownFilter.Predicates)
        {
            if (!dataFields.TryGetValue(predicate.ColumnPath, out DataField? field))
            {
                continue;
            }

            var pageReader = await rowGroupReader.OpenColumnPageReaderAsync(field, cancellationToken);
            lastOffsetIndex = pageReader.OffsetIndex;
            pageCount = Math.Max(pageCount, pageReader.PageCount);

            var hadPersistedColumnIndex = pageReader.ColumnIndex is not null;
            var columnIndex = await pageReader.GetColumnIndexAsync(cancellationToken);
            if (columnIndex is null)
            {
                continue;
            }

            pageIndexAvailable = true;
            usedFallback |= !hadPersistedColumnIndex;

            var predicateIntervals = GetCandidateIntervals(predicate, field, pageReader.OffsetIndex, columnIndex, rowGroupReader.RowCount);

            survivingIntervals = survivingIntervals is null
                ? predicateIntervals.Intervals
                : Intersect(survivingIntervals, predicateIntervals.Intervals);

            if (survivingIntervals.Count == 0)
            {
                break;
            }
        }

        var selectedPageCount = survivingIntervals is not null && lastOffsetIndex is not null
            ? SelectPageOrdinals(lastOffsetIndex, survivingIntervals, rowGroupReader.RowCount).Count
            : 0;

        return survivingIntervals is null
            ? new PagePruningResult(
                new[] { new RowInterval(0, rowGroupReader.RowCount) },
                pageCount,
                selectedPageCount: 0,
                pageIndexAvailable,
                usedFallback,
                source: pageIndexAvailable ? (usedFallback ? "fallback" : "persisted") : "unavailable",
                reason: pageIndexAvailable
                    ? "No plannable page-level statistics were available for the requested predicates."
                    : "Page indexes were unavailable for the requested predicates.")
            : new PagePruningResult(
                survivingIntervals,
                pageCount,
                selectedPageCount,
                pageIndexAvailable,
                usedFallback,
                source: pageIndexAvailable ? (usedFallback ? "fallback" : "persisted") : "unavailable",
                reason: pageIndexAvailable
                    ? selectedPageCount == pageCount
                        ? "Page indexes were available but could not narrow the surviving page set."
                        : "Page indexes narrowed the surviving page set."
                    : "Page indexes were unavailable for the requested predicates.");
    }

    public static bool IsFullCoverage(IReadOnlyList<RowInterval> intervals, long rowCount) =>
        intervals.Count == 1 &&
        intervals[0].Start == 0 &&
        intervals[0].End == rowCount;

    public static IReadOnlyList<int> ExpandRowIndexes(IReadOnlyList<RowInterval> intervals, int rowCount)
    {
        if (rowCount == 0)
        {
            return Array.Empty<int>();
        }

        var indexes = new List<int>(rowCount);
        foreach (var interval in intervals)
        {
            var start = (int)Math.Max(0, interval.Start);
            var end = (int)Math.Min(rowCount, interval.End);
            for (var index = start; index < end; index++)
            {
                indexes.Add(index);
            }
        }

        return indexes;
    }

    public static IReadOnlyList<RowInterval> ToIntervals(IReadOnlyList<int> rowIndexes)
    {
        if (rowIndexes.Count == 0)
        {
            return Array.Empty<RowInterval>();
        }

        var orderedIndexes = rowIndexes.OrderBy(index => index).ToArray();
        var intervals = new List<RowInterval>();
        var start = orderedIndexes[0];
        var previous = start;

        for (var index = 1; index < orderedIndexes.Length; index++)
        {
            var current = orderedIndexes[index];
            if (current == previous + 1)
            {
                previous = current;
                continue;
            }

            intervals.Add(new RowInterval(start, previous + 1L));
            start = previous = current;
        }

        intervals.Add(new RowInterval(start, previous + 1L));
        return intervals;
    }

    public static IReadOnlyList<int> SelectPageOrdinals(
        OffsetIndex offsetIndex,
        IReadOnlyList<RowInterval> intervals,
        long rowGroupRowCount)
    {
        if (intervals.Count == 0)
        {
            return Array.Empty<int>();
        }

        var pageOrdinals = new List<int>();
        for (var pageOrdinal = 0; pageOrdinal < offsetIndex.PageLocations.Count; pageOrdinal++)
        {
            var start = offsetIndex.PageLocations[pageOrdinal].FirstRowIndex;
            var end = pageOrdinal + 1 < offsetIndex.PageLocations.Count
                ? offsetIndex.PageLocations[pageOrdinal + 1].FirstRowIndex
                : rowGroupRowCount;

            if (Overlaps(intervals, start, end))
            {
                pageOrdinals.Add(pageOrdinal);
            }
        }

        return pageOrdinals;
    }

    private static CandidateIntervals GetCandidateIntervals<T>(
        PushdownPredicate<T> predicate,
        DataField field,
        OffsetIndex offsetIndex,
        ColumnIndex columnIndex,
        long rowGroupRowCount)
    {
        var intervals = new List<RowInterval>();
        var selectedPageCount = 0;

        for (var pageOrdinal = 0; pageOrdinal < offsetIndex.PageLocations.Count; pageOrdinal++)
        {
            if (!PageMayMatch(predicate, field, columnIndex, pageOrdinal))
            {
                continue;
            }

            var start = offsetIndex.PageLocations[pageOrdinal].FirstRowIndex;
            var end = pageOrdinal + 1 < offsetIndex.PageLocations.Count
                ? offsetIndex.PageLocations[pageOrdinal + 1].FirstRowIndex
                : rowGroupRowCount;
            intervals.Add(new RowInterval(start, end));
            selectedPageCount++;
        }

        return new CandidateIntervals(intervals, selectedPageCount);
    }

    private static bool PageMayMatch<T>(
        PushdownPredicate<T> predicate,
        DataField field,
        ColumnIndex columnIndex,
        int pageOrdinal)
    {
        if (columnIndex.NullPages.Count > pageOrdinal && columnIndex.NullPages[pageOrdinal])
        {
            return predicate is ComparisonPushdownPredicate<T> comparison &&
                comparison.Operator == ComparisonOperator.NotEqual;
        }

        return predicate switch
        {
            ComparisonPushdownPredicate<T> comparison => PageMayMatch(comparison, field, columnIndex, pageOrdinal),
            StartsWithPushdownPredicate<T> startsWith => PageMayMatch(startsWith, field, columnIndex, pageOrdinal),
            _ => true
        };
    }

    private static bool PageMayMatch<T>(
        ComparisonPushdownPredicate<T> predicate,
        DataField field,
        ColumnIndex columnIndex,
        int pageOrdinal)
    {
        object? minValue = DecodeIndexValue(field, columnIndex.MinValues[pageOrdinal]);
        object? maxValue = DecodeIndexValue(field, columnIndex.MaxValues[pageOrdinal]);
        if (minValue is null || maxValue is null)
        {
            return true;
        }

        return predicate.Operator switch
        {
            ComparisonOperator.Equal => Compare(predicate.Value, minValue) >= 0 && Compare(predicate.Value, maxValue) <= 0,
            ComparisonOperator.NotEqual => !(Compare(predicate.Value, minValue) == 0 && Compare(predicate.Value, maxValue) == 0 && GetNullCount(columnIndex, pageOrdinal) == 0),
            ComparisonOperator.LessThan => Compare(minValue, predicate.Value) < 0,
            ComparisonOperator.LessThanOrEqual => Compare(minValue, predicate.Value) <= 0,
            ComparisonOperator.GreaterThan => Compare(maxValue, predicate.Value) > 0,
            ComparisonOperator.GreaterThanOrEqual => Compare(maxValue, predicate.Value) >= 0,
            _ => true
        };
    }

    private static bool PageMayMatch<T>(
        StartsWithPushdownPredicate<T> predicate,
        DataField field,
        ColumnIndex columnIndex,
        int pageOrdinal)
    {
        var minValue = DecodeIndexValue(field, columnIndex.MinValues[pageOrdinal]) as string;
        var maxValue = DecodeIndexValue(field, columnIndex.MaxValues[pageOrdinal]) as string;
        if (minValue is null || maxValue is null)
        {
            return true;
        }

        var upperBound = GetOrdinalUpperBound(predicate.Prefix);
        return string.CompareOrdinal(maxValue, predicate.Prefix) >= 0 &&
            (upperBound is null || string.CompareOrdinal(minValue, upperBound) < 0);
    }

    private static long GetNullCount(ColumnIndex columnIndex, int pageOrdinal) =>
        columnIndex.NullCounts is not null && columnIndex.NullCounts.Count > pageOrdinal
            ? columnIndex.NullCounts[pageOrdinal]
            : 0;

    private static object? DecodeIndexValue(DataField field, byte[] encoded)
    {
        if (encoded.Length == 0)
        {
            return null;
        }

        var type = Nullable.GetUnderlyingType(field.ClrType) ?? field.ClrType;
        if (type.IsEnum)
        {
            var underlyingType = Enum.GetUnderlyingType(type);
            var rawValue = DecodePrimitive(underlyingType, encoded);
            return rawValue is null ? null : Enum.ToObject(type, rawValue);
        }

        return DecodePrimitive(type, encoded);
    }

    private static object? DecodePrimitive(System.Type type, byte[] encoded)
    {
        if (type == typeof(byte))
        {
            return encoded[0];
        }

        if (type == typeof(sbyte))
        {
            return unchecked((sbyte)encoded[0]);
        }

        if (type == typeof(short) && encoded.Length >= sizeof(short))
        {
            return BitConverter.ToInt16(encoded, 0);
        }

        if (type == typeof(ushort) && encoded.Length >= sizeof(ushort))
        {
            return BitConverter.ToUInt16(encoded, 0);
        }

        if (type == typeof(int) && encoded.Length >= sizeof(int))
        {
            return BitConverter.ToInt32(encoded, 0);
        }

        if (type == typeof(uint) && encoded.Length >= sizeof(uint))
        {
            return BitConverter.ToUInt32(encoded, 0);
        }

        if (type == typeof(long) && encoded.Length >= sizeof(long))
        {
            return BitConverter.ToInt64(encoded, 0);
        }

        if (type == typeof(ulong) && encoded.Length >= sizeof(ulong))
        {
            return BitConverter.ToUInt64(encoded, 0);
        }

        if (type == typeof(float) && encoded.Length >= sizeof(float))
        {
            return BitConverter.ToSingle(encoded, 0);
        }

        if (type == typeof(double) && encoded.Length >= sizeof(double))
        {
            return BitConverter.ToDouble(encoded, 0);
        }

        if (type == typeof(bool))
        {
            return encoded[0] != 0;
        }

        if (type == typeof(string))
        {
            return System.Text.Encoding.UTF8.GetString(encoded);
        }

        return null;
    }

    private static int Compare(object? left, object? right)
    {
        if (left is null || right is null)
        {
            return left == right ? 0 : 1;
        }

        var targetType = Nullable.GetUnderlyingType(left.GetType()) ?? left.GetType();
        var convertedRight = PushdownPredicateFactory.ConvertValue(right, targetType);
        if (left is IComparable comparable)
        {
            return comparable.CompareTo(convertedRight);
        }

        throw new NotSupportedException($"Values of type '{targetType.Name}' are not comparable.");
    }

    private static List<RowInterval> Intersect(IReadOnlyList<RowInterval> left, IReadOnlyList<RowInterval> right)
    {
        var result = new List<RowInterval>();
        var leftIndex = 0;
        var rightIndex = 0;

        while (leftIndex < left.Count && rightIndex < right.Count)
        {
            var start = Math.Max(left[leftIndex].Start, right[rightIndex].Start);
            var end = Math.Min(left[leftIndex].End, right[rightIndex].End);
            if (start < end)
            {
                result.Add(new RowInterval(start, end));
            }

            if (left[leftIndex].End <= right[rightIndex].End)
            {
                leftIndex++;
            }
            else
            {
                rightIndex++;
            }
        }

        return result;
    }

    private static bool Overlaps(IReadOnlyList<RowInterval> intervals, long start, long end)
    {
        foreach (var interval in intervals)
        {
            if (interval.End <= start)
            {
                continue;
            }

            if (interval.Start >= end)
            {
                break;
            }

            return true;
        }

        return false;
    }

    private static string? GetOrdinalUpperBound(string prefix)
    {
        if (string.IsNullOrEmpty(prefix))
        {
            return null;
        }

        var buffer = prefix.ToCharArray();
        for (var index = buffer.Length - 1; index >= 0; index--)
        {
            if (buffer[index] == char.MaxValue)
            {
                continue;
            }

            buffer[index]++;
            return new string(buffer, 0, index + 1);
        }

        return null;
    }

    private sealed record CandidateIntervals(IReadOnlyList<RowInterval> Intervals, int PageCount);
}

internal sealed record RowInterval(long Start, long End);

internal sealed class PagePruningResult
{
    public static PagePruningResult Full(long rowCount) => new(
        new[] { new RowInterval(0, rowCount) },
        pageCount: 0,
        selectedPageCount: 0,
        pageIndexAvailable: false,
        usedFallbackIndex: false,
        source: "unavailable",
        reason: "Page pruning was not applied.");

    public PagePruningResult(
        IReadOnlyList<RowInterval> intervals,
        int pageCount,
        int selectedPageCount,
        bool pageIndexAvailable,
        bool usedFallbackIndex,
        string source,
        string reason)
    {
        Intervals = intervals;
        PageCount = pageCount;
        SelectedPageCount = selectedPageCount;
        PageIndexAvailable = pageIndexAvailable;
        UsedFallbackIndex = usedFallbackIndex;
        Source = source;
        Reason = reason;
    }

    public IReadOnlyList<RowInterval> Intervals { get; }

    public int PageCount { get; }

    public int SelectedPageCount { get; }

    public bool PageIndexAvailable { get; }

    public bool UsedFallbackIndex { get; }

    public string Source { get; }

    public string Reason { get; }

    public long CandidateRowCountUpperBound => Intervals.Sum(interval => interval.End - interval.Start);
}
