namespace Parquet.Query.Extensions.Writing.Attributes;

/// <summary>
/// Marks a column as part of the parquet sort order.
/// </summary>
[AttributeUsage(AttributeTargets.Property | AttributeTargets.Field, AllowMultiple = false, Inherited = true)]
public sealed class ParquetSortKeyAttribute : Attribute
{
    /// <summary>
    /// Initializes a new sort-key attribute.
    /// </summary>
    /// <param name="priority">The sort priority, where lower values sort earlier.</param>
    public ParquetSortKeyAttribute(int priority)
    {
        Priority = priority;
    }

    /// <summary>
    /// Gets the sort priority, where lower values sort earlier.
    /// </summary>
    public int Priority { get; }

    /// <summary>
    /// Gets the relative order for the sort key.
    /// </summary>
    public int Order => Priority;

    /// <summary>
    /// Gets or sets the sort direction for the key.
    /// </summary>
    public ParquetSortDirection Direction { get; set; } = ParquetSortDirection.Ascending;
}
