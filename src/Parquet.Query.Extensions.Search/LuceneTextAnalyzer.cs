using System.Text;

namespace Parquet.Query.Extensions.Search;

internal static class LuceneTextAnalyzer
{
    public static IReadOnlyList<string> Analyze(string? value)
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

            Flush(buffer, tokens);
        }

        Flush(buffer, tokens);
        return tokens;
    }

    public static string AnalyzeSingleTerm(string term)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(term);

        var tokens = Analyze(term);
        if (tokens.Count != 1)
        {
            throw new ArgumentException(
                $"Lucene term queries require exactly one analyzed token. Received '{term}'.",
                nameof(term));
        }

        return tokens[0];
    }

    private static void Flush(StringBuilder buffer, ICollection<string> tokens)
    {
        if (buffer.Length == 0)
        {
            return;
        }

        tokens.Add(buffer.ToString());
        buffer.Clear();
    }
}
