namespace Parquet.Query.Extensions.Writing;

/// <summary>
/// Describes an index that should be created for a parquet column during writing.
/// </summary>
public sealed class ParquetIndexDescriptor
{
    /// <summary>
    /// Initializes a new index descriptor.
    /// </summary>
    /// <param name="kind">The logical kind of index to create.</param>
    /// <param name="memberPath">The CLR member path the index targets.</param>
    /// <param name="columnPath">The parquet column path the index targets.</param>
    /// <param name="strategyName">The external indexing strategy name, when applicable.</param>
    /// <param name="order">The relative order or priority for the index.</param>
    /// <param name="direction">The sort direction, when applicable.</param>
    public ParquetIndexDescriptor(
        ParquetIndexKind kind,
        string memberPath,
        string columnPath,
        string? strategyName,
        int order,
        ParquetSortDirection? direction)
    {
        Kind = kind;
        MemberPath = memberPath ?? throw new ArgumentNullException(nameof(memberPath));
        ColumnPath = columnPath ?? throw new ArgumentNullException(nameof(columnPath));
        StrategyName = strategyName;
        Order = order;
        Direction = direction;
    }

    /// <summary>
    /// Gets the logical kind of index to create.
    /// </summary>
    public ParquetIndexKind Kind { get; }

    /// <summary>
    /// Gets the CLR member path the index targets.
    /// </summary>
    public string MemberPath { get; }

    /// <summary>
    /// Gets the parquet column path the index targets.
    /// </summary>
    public string ColumnPath { get; }

    /// <summary>
    /// Gets the external indexing strategy name, when applicable.
    /// </summary>
    public string? StrategyName { get; }

    /// <summary>
    /// Gets the relative order or priority for the index.
    /// </summary>
    public int Order { get; }

    /// <summary>
    /// Gets the sort direction, when applicable.
    /// </summary>
    public ParquetSortDirection? Direction { get; }
}
