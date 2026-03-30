namespace Parquet.Query.Pushdown;

/// <summary>
/// Represents an immutable conjunction of pushdown predicates.
/// </summary>
/// <typeparam name="T">The source row type the predicates target.</typeparam>
public sealed class PushdownFilter<T>
{
    /// <summary>
    /// Gets an empty pushdown filter.
    /// </summary>
    public static PushdownFilter<T> Empty { get; } = new(Array.Empty<PushdownPredicate<T>>());

    /// <summary>
    /// Initializes a new filter from the provided predicates.
    /// </summary>
    /// <param name="predicates">The predicates to include.</param>
    public PushdownFilter(IEnumerable<PushdownPredicate<T>> predicates)
    {
        Predicates = predicates.ToArray();
    }

    /// <summary>
    /// Gets the predicates contained in the filter.
    /// </summary>
    public IReadOnlyList<PushdownPredicate<T>> Predicates { get; }

    /// <summary>
    /// Gets a value indicating whether the filter contains no predicates.
    /// </summary>
    public bool IsEmpty => Predicates.Count == 0;

    /// <summary>
    /// Combines this filter with another filter using logical AND semantics.
    /// </summary>
    /// <param name="other">The other filter to combine with this instance.</param>
    /// <returns>A new filter that contains predicates from both filters.</returns>
    public PushdownFilter<T> And(PushdownFilter<T> other)
    {
        ArgumentNullException.ThrowIfNull(other);
        return new PushdownFilter<T>(Predicates.Concat(other.Predicates));
    }

    internal bool Matches(T row) => Predicates.All(predicate => predicate.RowPredicate(row));
}
