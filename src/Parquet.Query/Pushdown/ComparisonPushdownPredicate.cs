namespace Parquet.Query.Pushdown;

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

    public ComparisonOperator Operator { get; }

    public object? Value { get; }

    public Type ValueType { get; }
}
