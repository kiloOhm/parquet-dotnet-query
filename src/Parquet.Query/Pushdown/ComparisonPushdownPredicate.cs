namespace Parquet.Query.Pushdown;

/// <summary>
/// Represents a scalar comparison predicate such as equality or range checks.
/// </summary>
/// <typeparam name="T">The source row type the predicate targets.</typeparam>
public sealed class ComparisonPushdownPredicate<T> : PushdownPredicate<T>
{
    internal ComparisonPushdownPredicate(
        string memberPath,
        string columnPath,
        ComparisonOperator @operator,
        object? value,
        Type valueType,
        string description,
        Func<T, bool> rowPredicate)
        : base(memberPath, columnPath, description, rowPredicate)
    {
        Operator = @operator;
        Value = value;
        ValueType = valueType;
    }

    /// <summary>
    /// Gets the comparison operator.
    /// </summary>
    public ComparisonOperator Operator { get; }

    /// <summary>
    /// Gets the comparison value.
    /// </summary>
    public object? Value { get; }

    /// <summary>
    /// Gets the static type of <see cref="Value"/>.
    /// </summary>
    public Type ValueType { get; }
}
