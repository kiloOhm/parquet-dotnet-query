namespace Parquet.Query.Extensions.Writing.Attributes;

[AttributeUsage(AttributeTargets.Property | AttributeTargets.Field, AllowMultiple = false, Inherited = true)]
public sealed class ParquetSortKeyAttribute : Attribute
{
    public ParquetSortKeyAttribute(int priority)
    {
        Priority = priority;
    }

    public int Priority { get; }

    public int Order => Priority;

    public ParquetSortDirection Direction { get; set; } = ParquetSortDirection.Ascending;
}
