namespace Parquet.Query.Planning;

internal static class PruningHelpers
{
    public static int CompareValues(
        object? left,
        object? right,
        bool allowNullEquality,
        string unsupportedTypeMessage)
    {
        if (left is null || right is null)
        {
            if (!allowNullEquality)
            {
                throw new InvalidOperationException("Cannot compare null values for statistics pruning.");
            }

            return left == right ? 0 : 1;
        }

        var targetType = Nullable.GetUnderlyingType(left.GetType()) ?? left.GetType();
        var convertedRight = PushdownPredicateFactory.ConvertValue(right, targetType);
        if (left is IComparable comparable)
        {
            return comparable.CompareTo(convertedRight);
        }

        throw new NotSupportedException(string.Format(unsupportedTypeMessage, targetType));
    }

    public static string? GetOrdinalUpperBound(string prefix)
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
