namespace Parquet.Query.Extensions.Writing;

public sealed class ParquetIndexDescriptor
{
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

    public ParquetIndexKind Kind { get; }

    public string MemberPath { get; }

    public string ColumnPath { get; }

    public string? StrategyName { get; }

    public int Order { get; }

    public ParquetSortDirection? Direction { get; }
}
