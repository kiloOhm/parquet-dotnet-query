namespace Parquet.Query.Extensions.Writing;

public sealed class ParquetColumnPlan
{
    public ParquetColumnPlan(
        string memberPath,
        string columnPath,
        Type memberType,
        IReadOnlyList<Attribute> attributes)
    {
        MemberPath = memberPath ?? throw new ArgumentNullException(nameof(memberPath));
        ColumnPath = columnPath ?? throw new ArgumentNullException(nameof(columnPath));
        MemberType = memberType ?? throw new ArgumentNullException(nameof(memberType));
        Attributes = attributes ?? throw new ArgumentNullException(nameof(attributes));
    }

    public string MemberPath { get; }

    public string ColumnPath { get; }

    public Type MemberType { get; }

    public IReadOnlyList<Attribute> Attributes { get; }
}
