namespace Parquet.Query.Pushdown;

/// <summary>
/// Identifies the comparison performed by a <see cref="ComparisonPushdownPredicate{T}"/>.
/// </summary>
public enum ComparisonOperator
{
    /// <summary>
    /// Values must be equal.
    /// </summary>
    Equal,
    /// <summary>
    /// Values must not be equal.
    /// </summary>
    NotEqual,
    /// <summary>
    /// The left side must be less than the right side.
    /// </summary>
    LessThan,
    /// <summary>
    /// The left side must be less than or equal to the right side.
    /// </summary>
    LessThanOrEqual,
    /// <summary>
    /// The left side must be greater than the right side.
    /// </summary>
    GreaterThan,
    /// <summary>
    /// The left side must be greater than or equal to the right side.
    /// </summary>
    GreaterThanOrEqual
}
