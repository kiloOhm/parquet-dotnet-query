using System.Linq.Expressions;
using Parquet.Query.Pushdown;

namespace Parquet.Query.Extensions.Search;

/// <summary>
/// Represents an analyzed Lucene term predicate that can optionally use fuzzy matching.
/// </summary>
/// <typeparam name="T">The source row type the predicate targets.</typeparam>
public sealed class LuceneTermPredicate<T> : PushdownPredicate<T>
    where T : class, new()
{
    /// <summary>
    /// Initializes a new Lucene term predicate.
    /// </summary>
    /// <param name="selector">The string member to search.</param>
    /// <param name="term">The term to match after analysis.</param>
    /// <param name="maxEdits">The maximum edit distance allowed.</param>
    /// <param name="prefixLength">The number of leading characters that must match exactly.</param>
    /// <param name="transpositions">Whether transposed characters count as a single edit.</param>
    public LuceneTermPredicate(
        Expression<Func<T, string?>> selector,
        string term,
        int maxEdits = 0,
        int prefixLength = 0,
        bool transpositions = true)
        : base(
            PushdownColumnPath.Resolve(selector).MemberPath,
            PushdownColumnPath.Resolve(selector).ColumnPath,
            Describe(selector, term, maxEdits, prefixLength, transpositions),
            CreateRowPredicate(selector, term, maxEdits, prefixLength, transpositions))
    {
        if (selector is null)
        {
            throw new ArgumentNullException(nameof(selector));
        }

        Term = LuceneTextAnalyzer.AnalyzeSingleTerm(term);
        MaxEdits = maxEdits;
        PrefixLength = prefixLength;
        Transpositions = transpositions;
    }

    /// <summary>
    /// Gets the analyzed search term.
    /// </summary>
    public string Term { get; }

    /// <summary>
    /// Gets the maximum edit distance allowed for fuzzy matching.
    /// </summary>
    public int MaxEdits { get; }

    /// <summary>
    /// Gets the number of leading characters that must match exactly.
    /// </summary>
    public int PrefixLength { get; }

    /// <summary>
    /// Gets a value indicating whether transposed characters count as a single edit.
    /// </summary>
    public bool Transpositions { get; }

    private static Func<T, bool> CreateRowPredicate(
        Expression<Func<T, string?>> selector,
        string term,
        int maxEdits,
        int prefixLength,
        bool transpositions)
    {
        var normalizedTerm = LuceneTextAnalyzer.AnalyzeSingleTerm(term);
        var compiledSelector = selector.Compile();

        return row => LuceneTextAnalyzer.Analyze(compiledSelector(row))
            .Any(token => LuceneEditDistance.IsMatch(token, normalizedTerm, maxEdits, prefixLength, transpositions));
    }

    private static string Describe(
        Expression<Func<T, string?>> selector,
        string term,
        int maxEdits,
        int prefixLength,
        bool transpositions)
    {
        var path = PushdownColumnPath.Resolve(selector);
        return maxEdits == 0
            ? $"lucene.match({path.MemberPath}, \"{term}\")"
            : $"lucene.fuzzy({path.MemberPath}, \"{term}\", maxEdits: {maxEdits}, prefixLength: {prefixLength}, transpositions: {transpositions.ToString().ToLowerInvariant()})";
    }
}
