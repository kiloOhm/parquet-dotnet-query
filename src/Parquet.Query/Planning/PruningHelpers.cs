namespace Parquet.Query.Planning;

internal static class PruningHelpers
{
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
