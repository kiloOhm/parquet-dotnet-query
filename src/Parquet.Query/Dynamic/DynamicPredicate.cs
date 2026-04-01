namespace Parquet.Query.Dynamic;

/// <summary>
/// Describes a filter predicate for a dynamic (non-generic) parquet query.
/// </summary>
public sealed class DynamicPredicate
{
    /// <summary>
    /// Initializes a new dynamic predicate.
    /// </summary>
    /// <param name="column">The parquet column path.</param>
    /// <param name="operator">The comparison operator (e.g. "==", "!=", "&lt;", "&gt;=", "between", "startsWith", "LuceneMatch", "LuceneFuzzy").</param>
    /// <param name="value">The comparison value as a string, parsed according to the column's schema type.</param>
    /// <param name="value2">A second value for range operators like "between".</param>
    /// <param name="maxEdits">Maximum edit distance for fuzzy matching (0-2). Defaults to 0 for LuceneMatch, 1 for LuceneFuzzy.</param>
    /// <param name="prefixLength">Required prefix length for fuzzy matching.</param>
    /// <param name="transpositions">Whether to count transpositions as a single edit.</param>
    public DynamicPredicate(
        string column,
        string @operator,
        string? value = null,
        string? value2 = null,
        int? maxEdits = null,
        int? prefixLength = null,
        bool? transpositions = null)
    {
        Column = column ?? throw new ArgumentNullException(nameof(column));
        Operator = @operator ?? throw new ArgumentNullException(nameof(@operator));
        Value = value;
        Value2 = value2;
        MaxEdits = maxEdits;
        PrefixLength = prefixLength;
        Transpositions = transpositions;
    }

    /// <summary>
    /// Gets the parquet column path.
    /// </summary>
    public string Column { get; }

    /// <summary>
    /// Gets the comparison operator.
    /// </summary>
    public string Operator { get; }

    /// <summary>
    /// Gets the primary comparison value.
    /// </summary>
    public string? Value { get; }

    /// <summary>
    /// Gets the secondary value for range operators.
    /// </summary>
    public string? Value2 { get; }

    /// <summary>
    /// Gets the maximum edit distance for fuzzy matching.
    /// </summary>
    public int? MaxEdits { get; }

    /// <summary>
    /// Gets the required prefix length for fuzzy matching.
    /// </summary>
    public int? PrefixLength { get; }

    /// <summary>
    /// Gets whether transpositions count as a single edit.
    /// </summary>
    public bool? Transpositions { get; }
}
