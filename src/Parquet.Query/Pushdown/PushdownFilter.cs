namespace Parquet.Query.Pushdown;

public sealed class PushdownFilter<T>
{
    public static PushdownFilter<T> Empty { get; } = new(Array.Empty<PushdownPredicate<T>>());

    public PushdownFilter(IEnumerable<PushdownPredicate<T>> predicates)
    {
        Predicates = predicates.ToArray();
    }

    public IReadOnlyList<PushdownPredicate<T>> Predicates { get; }

    public bool IsEmpty => Predicates.Count == 0;

    public PushdownFilter<T> And(PushdownFilter<T> other)
    {
        ArgumentNullException.ThrowIfNull(other);
        return new PushdownFilter<T>(Predicates.Concat(other.Predicates));
    }

    internal bool Matches(T row) => Predicates.All(predicate => predicate.RowPredicate(row));
}
