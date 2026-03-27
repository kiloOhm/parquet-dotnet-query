namespace Parquet.Query.Extensions.Writing.Attributes;

[AttributeUsage(AttributeTargets.Property | AttributeTargets.Field, AllowMultiple = false, Inherited = true)]
public sealed class ParquetSortKeyAttribute : Attribute
{
    public ParquetSortKeyAttribute(int order)
    {
        Order = order;
    }

    public int Order { get; }

    public ParquetSortDirection Direction { get; set; } = ParquetSortDirection.Ascending;
}
