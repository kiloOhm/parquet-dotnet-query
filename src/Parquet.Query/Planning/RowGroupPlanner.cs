using Parquet;
using Parquet.Data;
using Parquet.Meta;
using Parquet.Query.Internal;
using Parquet.Query.Pushdown;
using Parquet.Schema;

namespace Parquet.Query.Planning;

internal static class RowGroupPlanner
{
    public static async Task<QueryFilePlan> BuildFilePlanAsync<T>(
        string filePath,
        ParquetReader reader,
        PushdownFilter<T> pushdownFilter,
        IReadOnlyList<FilePredicateDecision> fileDecisions,
        IReadOnlyList<IParquetPredicatePlanner<T>> predicatePlanners,
        CancellationToken cancellationToken)
        where T : class, new()
    {
        var dataFields = reader.Schema.GetDataFields()
            .ToDictionary(field => field.Path.ToString(), StringComparer.Ordinal);

        var rowGroups = new List<RowGroupPlan>(reader.RowGroupCount);
        var anyPageIndex = false;
        for (var rowGroupIndex = 0; rowGroupIndex < reader.RowGroupCount; rowGroupIndex++)
        {
            using var rowGroupReader = reader.OpenRowGroupReader(rowGroupIndex);
            var decisions = new List<RowGroupPredicateDecision>(pushdownFilter.Predicates.Count);
            var shouldRead = true;
            var pageIndexAvailable = HasPersistedPageIndex(rowGroupReader, dataFields.Values);
            anyPageIndex |= pageIndexAvailable;
            var pagePruning = PagePruningResult.Full(rowGroupReader.RowCount);
            var plannerContext = new ParquetRowGroupPlannerContext(
                filePath,
                rowGroupIndex,
                reader,
                rowGroupReader,
                reader.Schema,
                dataFields);

            foreach (var predicate in pushdownFilter.Predicates)
            {
                var decision = EvaluatePredicate(predicate, plannerContext, predicatePlanners);
                decisions.Add(decision);

                if (!decision.MayMatch)
                {
                    shouldRead = false;
                }
            }

            if (shouldRead && !pushdownFilter.IsEmpty)
            {
                pagePruning = await PagePruner.PruneAsync(
                    new ParquetPagePruningContext(
                        filePath,
                        rowGroupIndex,
                        reader,
                        rowGroupReader,
                        reader.Schema,
                        dataFields),
                    pushdownFilter,
                    predicatePlanners,
                    cancellationToken).ConfigureAwait(false);
                pageIndexAvailable |= pagePruning.PageIndexAvailable;
                anyPageIndex |= pagePruning.PageIndexAvailable;
                if (pagePruning.CandidateRowCountUpperBound == 0)
                {
                    shouldRead = false;
                }
            }

            rowGroups.Add(new RowGroupPlan(
                filePath,
                rowGroupIndex,
                rowGroupReader.RowCount,
                shouldRead,
                pageIndexAvailable,
                decisions,
                pagePruning.PageCount,
                pagePruning.SelectedPageCount,
                shouldRead ? pagePruning.CandidateRowCountUpperBound : 0,
                pagePruning.UsedFallbackIndex,
                pagePruning.Source,
                pagePruning.Reason,
                pagePruning.Intervals));
        }

        var fileShouldRead = fileDecisions.All(decision => decision.MayMatch) && rowGroups.Any(rowGroup => rowGroup.ShouldRead);
        var reason = !fileDecisions.All(decision => decision.MayMatch)
            ? "Path partitions ruled the file out."
            : rowGroups.Any(rowGroup => rowGroup.ShouldRead)
                ? "At least one row group may match."
                : "All row groups were ruled out by metadata.";

        return new QueryFilePlan(
            filePath,
            fileShouldRead,
            reason,
            fileDecisions,
            rowGroups,
            anyPageIndex);
    }

    private static bool HasPersistedPageIndex(IParquetRowGroupReader rowGroupReader, IEnumerable<DataField> fields)
    {
        foreach (var field in fields)
        {
            if (!rowGroupReader.ColumnExists(field))
            {
                continue;
            }

            ColumnChunk? metadata = rowGroupReader.GetMetadata(field);
            if (metadata is null)
            {
                continue;
            }

            if (metadata.ColumnIndexOffset.HasValue &&
                metadata.ColumnIndexLength.HasValue &&
                metadata.OffsetIndexOffset.HasValue &&
                metadata.OffsetIndexLength.HasValue &&
                metadata.ColumnIndexLength.Value > 0 &&
                metadata.OffsetIndexLength.Value > 0)
            {
                return true;
            }
        }

        return false;
    }

    private static RowGroupPredicateDecision EvaluatePredicate<T>(
        PushdownPredicate<T> predicate,
        ParquetRowGroupPlannerContext context,
        IReadOnlyList<IParquetPredicatePlanner<T>> predicatePlanners)
        where T : class, new()
    {
        if (!context.DataFields.TryGetValue(predicate.ColumnPath, out DataField? field))
        {
            return TryEvaluateWithExtensions(predicate, context, predicatePlanners)
                ?? new RowGroupPredicateDecision(
                    predicate.Description,
                    mayMatch: true,
                    source: "schema",
                    reason: $"Column '{predicate.ColumnPath}' was not found in the file schema.");
        }

        var builtInDecision = predicate switch
        {
            ComparisonPushdownPredicate<T> comparison => EvaluateComparison(comparison, context.RowGroupReader, field),
            StartsWithPushdownPredicate<T> startsWith => EvaluateStartsWith(startsWith, context.RowGroupReader, field),
            _ => null
        };

        if (builtInDecision is not null && !builtInDecision.MayMatch)
        {
            return builtInDecision;
        }

        var extensionDecision = TryEvaluateWithExtensions(predicate, context, predicatePlanners);
        if (extensionDecision is null)
        {
            return builtInDecision
                ?? new RowGroupPredicateDecision(predicate.Description, true, "pushdown", "Predicate type is not plannable.");
        }

        return builtInDecision is null
            ? extensionDecision
            : CombineDecisions(builtInDecision, extensionDecision);
    }

    private static RowGroupPredicateDecision? TryEvaluateWithExtensions<T>(
        PushdownPredicate<T> predicate,
        ParquetRowGroupPlannerContext context,
        IReadOnlyList<IParquetPredicatePlanner<T>> predicatePlanners)
        where T : class, new()
    {
        foreach (var planner in predicatePlanners)
        {
            if (!planner.CanPlan(predicate))
            {
                continue;
            }

            var decision = planner.TryEvaluateRowGroup(context, predicate);
            if (decision is not null)
            {
                return decision;
            }
        }

        return null;
    }

    private static RowGroupPredicateDecision EvaluateComparison<T>(
        ComparisonPushdownPredicate<T> predicate,
        IParquetRowGroupReader rowGroupReader,
        DataField field)
    {
        var statistics = rowGroupReader.GetStatistics(field);
        if (TryRuleOutWithStatistics(predicate, statistics, out string? reason))
        {
            return new RowGroupPredicateDecision(predicate.Description, false, "statistics", reason!);
        }

        if (predicate.Operator == ComparisonOperator.Equal && predicate.Value is not null)
        {
            try
            {
                if (!rowGroupReader.MightMatchEquals(field, predicate.Value))
                {
                    return new RowGroupPredicateDecision(
                        predicate.Description,
                        false,
                        "bloom",
                        "Bloom filter ruled the equality predicate out.");
                }

                return new RowGroupPredicateDecision(
                    predicate.Description,
                    true,
                    "statistics+bloom",
                    statistics is null
                        ? "Bloom filter did not rule the row group out."
                        : "Statistics and bloom filter did not rule the row group out.");
            }
            catch (Exception exception)
            {
                return new RowGroupPredicateDecision(
                    predicate.Description,
                    true,
                    "bloom",
                    $"Bloom filter was unavailable: {exception.Message}");
            }
        }

        return new RowGroupPredicateDecision(
            predicate.Description,
            true,
            "statistics",
            statistics is null
                ? "No statistics were available for this column chunk."
                : "Statistics could not rule the row group out.");
    }

    private static RowGroupPredicateDecision CombineDecisions(
        RowGroupPredicateDecision builtInDecision,
        RowGroupPredicateDecision extensionDecision)
    {
        var source = string.Equals(builtInDecision.Source, extensionDecision.Source, StringComparison.Ordinal)
            ? builtInDecision.Source
            : $"{builtInDecision.Source}+{extensionDecision.Source}";

        if (!extensionDecision.MayMatch)
        {
            return new RowGroupPredicateDecision(
                builtInDecision.Predicate,
                mayMatch: false,
                source,
                extensionDecision.Reason);
        }

        var reason = string.Equals(builtInDecision.Reason, extensionDecision.Reason, StringComparison.Ordinal)
            ? builtInDecision.Reason
            : $"{builtInDecision.Reason} {extensionDecision.Reason}".Trim();

        return new RowGroupPredicateDecision(
            builtInDecision.Predicate,
            mayMatch: true,
            source,
            reason);
    }

    private static RowGroupPredicateDecision EvaluateStartsWith<T>(
        StartsWithPushdownPredicate<T> predicate,
        IParquetRowGroupReader rowGroupReader,
        DataField field)
    {
        var statistics = rowGroupReader.GetStatistics(field);
        if (statistics?.MinValue is not string minValue || statistics.MaxValue is not string maxValue)
        {
            return new RowGroupPredicateDecision(
                predicate.Description,
                true,
                "statistics",
                "String min/max statistics were not available.");
        }

        var upperBound = GetOrdinalUpperBound(predicate.Prefix);
        var mayMatch = string.CompareOrdinal(maxValue, predicate.Prefix) >= 0 &&
            (upperBound is null || string.CompareOrdinal(minValue, upperBound) < 0);

        return new RowGroupPredicateDecision(
            predicate.Description,
            mayMatch,
            "statistics",
            mayMatch
                ? "String min/max statistics overlap the requested prefix range."
                : "String min/max statistics rule the prefix range out.");
    }

    private static bool TryRuleOutWithStatistics<T>(
        ComparisonPushdownPredicate<T> predicate,
        DataColumnStatistics? statistics,
        out string? reason)
    {
        reason = null;

        if (statistics is null)
        {
            return false;
        }

        switch (predicate.Operator)
        {
            case ComparisonOperator.Equal:
                if (predicate.Value is not null &&
                    ((statistics.MinValue is not null && CompareValues(predicate.Value, statistics.MinValue) < 0) ||
                     (statistics.MaxValue is not null && CompareValues(predicate.Value, statistics.MaxValue) > 0)))
                {
                    reason = "The equality constant falls outside the row group's min/max range.";
                    return true;
                }

                return false;

            case ComparisonOperator.NotEqual:
                if (predicate.Value is not null &&
                    statistics.MinValue is not null &&
                    statistics.MaxValue is not null &&
                    statistics.NullCount.GetValueOrDefault() == 0 &&
                    CompareValues(predicate.Value, statistics.MinValue) == 0 &&
                    CompareValues(predicate.Value, statistics.MaxValue) == 0)
                {
                    reason = "All non-null values in the row group are equal to the excluded value.";
                    return true;
                }

                return false;

            case ComparisonOperator.LessThan:
                if (statistics.MinValue is not null && CompareValues(statistics.MinValue, predicate.Value) >= 0)
                {
                    reason = "The row group's minimum value is already above the exclusive upper bound.";
                    return true;
                }

                return false;

            case ComparisonOperator.LessThanOrEqual:
                if (statistics.MinValue is not null && CompareValues(statistics.MinValue, predicate.Value) > 0)
                {
                    reason = "The row group's minimum value is above the inclusive upper bound.";
                    return true;
                }

                return false;

            case ComparisonOperator.GreaterThan:
                if (statistics.MaxValue is not null && CompareValues(statistics.MaxValue, predicate.Value) <= 0)
                {
                    reason = "The row group's maximum value is already below the exclusive lower bound.";
                    return true;
                }

                return false;

            case ComparisonOperator.GreaterThanOrEqual:
                if (statistics.MaxValue is not null && CompareValues(statistics.MaxValue, predicate.Value) < 0)
                {
                    reason = "The row group's maximum value is below the inclusive lower bound.";
                    return true;
                }

                return false;

            default:
                return false;
        }
    }

    private static int CompareValues(object? left, object? right)
    {
        if (left is null || right is null)
        {
            throw new InvalidOperationException("Cannot compare null values for statistics pruning.");
        }

        var targetType = Nullable.GetUnderlyingType(left.GetType()) ?? left.GetType();
        var convertedRight = PushdownPredicateFactory.ConvertValue(right, targetType);

        if (left is IComparable comparable)
        {
            return comparable.CompareTo(convertedRight);
        }

        throw new NotSupportedException($"Values of type '{targetType}' are not comparable.");
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
}
