using System.Linq.Expressions;
using Parquet.Query.Pushdown;

namespace Parquet.Query.Extensions.Search;

public sealed class LuceneTermPredicate<T> : PushdownPredicate<T>
    where T : class, new()
{
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
        ArgumentNullException.ThrowIfNull(selector);

        Term = LuceneTextAnalyzer.AnalyzeSingleTerm(term);
        MaxEdits = maxEdits;
        PrefixLength = prefixLength;
        Transpositions = transpositions;
    }

    public string Term { get; }

    public int MaxEdits { get; }

    public int PrefixLength { get; }

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
