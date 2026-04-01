using Parquet.Query.Pushdown;

namespace Parquet.Query.Dynamic;

/// <summary>
/// Represents a Lucene term predicate for dynamic queries that can participate
/// in row-group pruning via footer Lucene indexes.
/// </summary>
internal sealed class DynamicLuceneTermPredicate : PushdownPredicate<DynamicRow>
{
    public DynamicLuceneTermPredicate(
        string columnPath,
        string term,
        int maxEdits,
        int prefixLength,
        bool transpositions,
        string description,
        Func<DynamicRow, bool> rowPredicate)
        : base(columnPath, columnPath, description, rowPredicate)
    {
        Term = term;
        MaxEdits = maxEdits;
        PrefixLength = prefixLength;
        Transpositions = transpositions;
    }

    public string Term { get; }

    public int MaxEdits { get; }

    public int PrefixLength { get; }

    public bool Transpositions { get; }
}
