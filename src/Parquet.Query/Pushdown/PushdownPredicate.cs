namespace Parquet.Query.Pushdown;

/// <summary>
/// Represents a predicate that can participate in parquet pushdown planning and row evaluation.
/// </summary>
/// <typeparam name="T">The source row type the predicate targets.</typeparam>
public abstract class PushdownPredicate<T>
{
    /// <summary>
    /// Initializes a new predicate.
    /// </summary>
    /// <param name="memberPath">The source member path referenced by the predicate.</param>
    /// <param name="columnPath">The parquet column path referenced by the predicate.</param>
    /// <param name="description">A human-readable description of the predicate.</param>
    /// <param name="rowPredicate">The compiled predicate used for row-level evaluation.</param>
    protected PushdownPredicate(
        string memberPath,
        string columnPath,
        string description,
        Func<T, bool> rowPredicate)
    {
        MemberPath = memberPath;
        ColumnPath = columnPath;
        Description = description;
        RowPredicate = rowPredicate;
    }

    /// <summary>
    /// Gets the source member path referenced by the predicate.
    /// </summary>
    public string MemberPath { get; }

    /// <summary>
    /// Gets the parquet column path referenced by the predicate.
    /// </summary>
    public string ColumnPath { get; }

    /// <summary>
    /// Gets a human-readable description of the predicate.
    /// </summary>
    public string Description { get; }

    internal Func<T, bool> RowPredicate { get; }
}
