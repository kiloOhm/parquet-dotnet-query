using Parquet.Query.Pushdown;

namespace Parquet.Query.Planning;

internal static class PartitionPruner
{
    public static IReadOnlyList<FilePredicateDecision> Evaluate<T>(
        string filePath,
        IReadOnlyList<PushdownPredicate<T>> predicates)
    {
        var partitionValues = ExtractPartitionValues(filePath);
        if (partitionValues.Count == 0 || predicates.Count == 0)
        {
            return Array.Empty<FilePredicateDecision>();
        }

        var decisions = new List<FilePredicateDecision>(predicates.Count);
        foreach (var predicate in predicates)
        {
            var decision = EvaluatePredicate(predicate, partitionValues);
            if (decision is not null)
            {
                decisions.Add(decision);
            }
        }

        return decisions;
    }

    private static FilePredicateDecision? EvaluatePredicate<T>(
        PushdownPredicate<T> predicate,
        IReadOnlyDictionary<string, string> partitionValues)
    {
        if (!TryGetPartitionValue(partitionValues, predicate.MemberPath, out var rawValue))
        {
            return null;
        }

        return predicate switch
        {
            ComparisonPushdownPredicate<T> comparison => EvaluateComparison(comparison, rawValue),
            StartsWithPushdownPredicate<T> startsWith => new FilePredicateDecision(
                predicate.Description,
                rawValue.StartsWith(startsWith.Prefix, StringComparison.Ordinal),
                "partition",
                $"Path partition value '{rawValue}' was compared against prefix '{startsWith.Prefix}'."),
            _ => null
        };
    }

    private static FilePredicateDecision EvaluateComparison<T>(
        ComparisonPushdownPredicate<T> predicate,
        string rawValue)
    {
        try
        {
            var typedValue = PushdownPredicateFactory.ConvertValue(rawValue, predicate.ValueType);
            var comparison = CompareValues(typedValue, predicate.Value);
            var mayMatch = predicate.Operator switch
            {
                ComparisonOperator.Equal => comparison == 0,
                ComparisonOperator.NotEqual => comparison != 0,
                ComparisonOperator.LessThan => comparison < 0,
                ComparisonOperator.LessThanOrEqual => comparison <= 0,
                ComparisonOperator.GreaterThan => comparison > 0,
                ComparisonOperator.GreaterThanOrEqual => comparison >= 0,
                _ => true
            };

            return new FilePredicateDecision(
                predicate.Description,
                mayMatch,
                "partition",
                $"Path partition value '{rawValue}' was evaluated before opening the file.");
        }
        catch (Exception exception)
        {
            return new FilePredicateDecision(
                predicate.Description,
                mayMatch: true,
                source: "partition",
                reason: $"Partition value '{rawValue}' could not be interpreted: {exception.Message}");
        }
    }

    private static int CompareValues(object? left, object? right)
    {
        if (left is null || right is null)
        {
            return left == right ? 0 : 1;
        }

        if (left is IComparable comparable)
        {
            var targetType = Nullable.GetUnderlyingType(left.GetType()) ?? left.GetType();
            var convertedRight = PushdownPredicateFactory.ConvertValue(right, targetType);
            return comparable.CompareTo(convertedRight);
        }

        throw new NotSupportedException($"Values of type '{left.GetType().Name}' are not comparable for partition pruning.");
    }

    private static IReadOnlyDictionary<string, string> ExtractPartitionValues(string filePath)
    {
        var result = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        var directory = Path.GetDirectoryName(filePath);
        if (string.IsNullOrWhiteSpace(directory))
        {
            return result;
        }

        foreach (var segment in directory.Split(Path.DirectorySeparatorChar, Path.AltDirectorySeparatorChar))
        {
            var separatorIndex = segment.IndexOf('=');
            if (separatorIndex <= 0 || separatorIndex == segment.Length - 1)
            {
                continue;
            }

            var key = segment[..separatorIndex];
            var value = segment[(separatorIndex + 1)..];
            result[key] = value;
        }

        return result;
    }

    private static bool TryGetPartitionValue(
        IReadOnlyDictionary<string, string> partitionValues,
        string memberPath,
        out string value)
    {
        if (partitionValues.TryGetValue(memberPath, out value!))
        {
            return true;
        }

        var separatorIndex = memberPath.LastIndexOf('.');
        if (separatorIndex >= 0)
        {
            return partitionValues.TryGetValue(memberPath[(separatorIndex + 1)..], out value!);
        }

        return false;
    }
}
