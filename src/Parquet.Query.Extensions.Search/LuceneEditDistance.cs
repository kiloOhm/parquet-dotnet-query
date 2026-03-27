namespace Parquet.Query.Extensions.Search;

internal static class LuceneEditDistance
{
    public static bool IsMatch(
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

        var candidateSuffix = candidate[prefixLength..];
        var querySuffix = query[prefixLength..];

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
}
